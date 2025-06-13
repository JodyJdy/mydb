from typing import Tuple, List

import typing

from store.container import Container
from store.page import Record, CommonPage
from values import Row, generate_row, IntValue, Value

# 叶子/分支节点都包含的信息
class ControlRow:
    def __init__(self,node_type:int=None,parent:int=None,left:int=None,right:int=None):
        self.node_type = node_type
        self.parent = parent
        self.left = left
        self.right = right
    def to_row(self):
        return generate_row([self.node_type,self.parent,self.left,self.right])
    def is_root(self):
        return self.parent == -1

class BranchRow:
    def __init__(self,key:Row,child:int):
        self.key = key
        self.child = child
    def to_row(self):
        result = self.key.values.copy()
        result.append(IntValue(self.child))
        return result


LEAF_NODE = 0
BRANCH_NODE = 1
class Node:
    def __init__(self,page:CommonPage):
        self.page = page
        self.control_row:ControlRow|None = None
        self.tree = None
    def row_num(self):
        """
        去掉 control row
        :return:
        """
        return self.page.slot_num - 1
    def key_index(self,key):
        pass
    def child_index(self,child)->int:
        pass
    def  find_index_for_key_insert(self,k)->int:
        pass
    def get_row_i(self,i:int):
        pass
    def get_last_row(self):
        pass

    def parent(self)->int:
        return self.control_row.parent
    def set_parent(self,parent:int):
        self.control_row.parent = parent
    def left(self):
        return self.control_row.left
    def right(self):
        return self.control_row.right
    def set_left(self,left:int):
        self.control_row.left = left
    def set_right(self,right:int):
        self.control_row.right = right
    def sync(self):
        """
        同步control row
        :return:
        """
        self.page.update_by_slot(self.control_row.to_row(),0)
class LeafNode(Node):
    def __init__(self,page:CommonPage):
        super().__init__(page)

    def __repr__(self):
        return f'{self.rows}'

    def key_index(self, key):
        #key值相等的地方
        eq_index = None
        #符合插入条件的地方
        insert_index = None
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            if  eq_index is None  and   key == row_i:
                eq_index = i
            if insert_index is None and key < row_i:
                insert_index = i
            if eq_index and insert_index:
                break
        if eq_index is None:
            eq_index = -1
        if insert_index is None:
            insert_index = self.row_num()
        return  eq_index,insert_index
    def  find_index_for_key_insert(self,key):
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            if  key < row_i:
                return i
        return self.row_num()


    def get_row_i(self, i: int)->Row:
        """
        b tree中的下标真正使用时，要 + 1， 因为 每页里面都有  control row

        :param i:
        :return:
        """
        btree:BTree = self.tree
        return  btree.parse_leaf_row(self.page.read_slot(i+1))

    def get_last_row(self):
        btree:BTree = self.tree
        return  btree.parse_leaf_row(self.page.read_slot(self.page.slot_num - 1))
    def update_row_i(self,i,value:Row):
        self.page.update_by_slot(value,i+1)
    def insert_row(self,i:int,value:Row):
        result,_ = self.page.insert_slot(value,i+1)
        if result == -1:
            return False
        return True

class BranchNode(Node):
    def __init__(self,page:CommonPage):
        super().__init__(page)

    def key_index(self, key):
        for index,row in enumerate(self.rows):
            if key == row.key:
                return index
        return -1
    def child_index(self,child):
        for index,row in enumerate(self.rows):
            if child == row.child:
                return index
        return -1
    def  find_index_for_key_insert(self,k):
        for index,row in enumerate(self.rows):
            if not row.key:
                continue
            if k< row.key:
                return index
        return self.row_num()

    def get_row_i(self, i: int)->BranchRow:
        """
        b tree中的下标真正使用时，要 + 1， 因为 每页里面都有  control row

        :param i:
        :return:
        """
        btree:BTree = self.tree
        return  btree.parse_branch_row(self.page.read_slot(i+1))

    def get_last_row(self):
        btree:BTree = self.tree
        return btree.parse_branch_row(self.page.read_slot(self.page.slot_num - 1))

class BTree:
    def __init__(self, name:str, key_len:int, value_types:List[typing.Type[Value]], duplicate_key=False):
        self.container = Container(name)
        self.tree = self.create_leaf_node(-1)
        self.key_len = key_len
        self.value_type = value_types
        self.duplicate_key = duplicate_key

    def create_leaf_node(self,parent:int):
        page = self.container.new_common_page()
        leaf = LeafNode(page)
        leaf.control_row = ControlRow(LEAF_NODE,parent,-1,-1)
        page.insert_to_last_slot(leaf.control_row.to_row())
        leaf.tree = self
        return leaf

    def read_node(self,page_num:int) -> LeafNode:
        page = self.container.get_page(page_num)
        control_row = self.parse_control_row(page.read_slot(0))
        if control_row.node_type == LEAF_NODE:
            node = LeafNode(page)
        else:
            node = BranchNode(page)
        node.control_row = self.parse_control_row(page.read_slot(0))
        node.tree = self
        return node

    def read_branch_node(self,page_num:int) -> BranchNode:
        page = self.container.get_page(page_num)
        branch = BranchNode(page)
        branch.control_row = self.parse_control_row(page.read_slot(0))
        branch.tree = self
        return branch

    def create_branch_node(self,parent:int):
        page = self.container.new_common_page()
        branch = BranchNode(page)
        branch.control_row = ControlRow(BRANCH_NODE,parent,-1,-1)
        page.insert_to_last_slot(branch.control_row.to_row())
        branch.tree = self
        return branch

    def parse_control_row(self,record:Record):
        if not record.fields == 4:
            raise Exception(f'record内容与数据类型不匹配')
        node_type = IntValue.from_bytes(record.fields[0].value).value
        parent = IntValue.from_bytes(record.fields[1].value).value
        left = IntValue.from_bytes(record.fields[2].value).value
        right = IntValue.from_bytes(record.fields[3].value).value
        return ControlRow(node_type=node_type,parent=parent,left=left,right=right)

    def parse_branch_row(self,record:Record):
        if not len(record.fields) == self.key_len + 1:
            raise Exception(f'record内容与数据类型不匹配')
        #第一位是 child
        child = IntValue.from_bytes(record.fields[0].value).value
        #读取key的内容
        key = []
        for i in range(self.key_len):
            key.append(self.value_type[i].from_bytes(record.fields[i+1].value))
        return BranchRow(Row(key),child)
    def parse_leaf_row(self,record:Record):
        if not len(record.fields) == len(self.value_type):
            raise Exception(f'record内容与数据类型不匹配')
        result = []
        for value_type,field in zip(self.value_type,record.fields):
            if field.is_null():
                result.append(value_type.none())
            else:
                result.append(value_type.from_bytes(field.value))
        return Row(result)

    def search(self,key):
        node =self._search(key,self.tree)
        return node

    def _search(self,key,node,for_insert = False)->LeafNode|None:
        #叶子节点直接返回
        while not isinstance(node,LeafNode):
            branch_node:BranchNode = node
            #branchnode 第0行数据的key是不使用的仅占位用
            i = 1
            row_num = branch_node.row_num()
            while i < row_num:
                #如果允许重复插入，新节点，插入左侧节点
                cur_row_key = branch_node.get_row_i(i).key
                if key < cur_row_key or (for_insert and key == cur_row_key and self.duplicate_key):
                    node = self.read_node(branch_node.get_row_i(i-1).child)
                    break
                elif key == cur_row_key:
                    node = self.read_node(branch_node.get_row_i(i).child)
                    break
                i+=1
            if i == row_num:
                node = self.read_node(branch_node.get_last_row().child)
        return node


    def insert(self,value):
        key = self.get_key(value)
        #找到叶子节点
        node:LeafNode = self._search(key,self.tree,True)
        #查找key相等的部分，如果不存在，就找可以插入的部分
        eq_index,insert_index = node.key_index(key)
        if eq_index != -1 and not self.duplicate_key:
            node.update_row_i(eq_index,value)
            return
        #校验，直接进入插入，如果插入失败，说明满了
        if  node.insert_row(insert_index,value):
            return
        #溢出了，进行split,  将 FULL-1 的部分平分
        self.split_leaf_node(node, value, insert_index)

    def create_root(self,old_root,right_node,key):
        old_root.is_root = False
        root = BranchNode(None,True)
        root.rows.append(BranchRow(None,old_root))
        root.rows.append(BranchRow(key,right_node))
        old_root.parent = root
        right_node.parent = root
        self.tree = root

    def get_key(self,row)->Row:
        if isinstance(row,Row):
            return Row(row.values[0:self.key_len])
        elif isinstance(row,BranchRow):
            return row.key
        else:
            raise Exception('不支持获取key')

    def split_leaf_node(self, node:LeafNode, value, index):

        mid = (node.row_num() + 1)//2
        #为了分配left个节点，需要计算从node 中取的内容
        if mid > index:
            mid = mid - 1

        right_node = self.create_leaf_node(node.parent())
        right_node.set_right(node.right())
        if node.right() != -1:
            node.right.left = right_node
        right_node.left = node
        node.right = right_node

        #切分出右边的节点
        right_node.rows = node.rows[mid:]
        #调整左边节点的数据
        node.rows = node.rows[0:mid]

        if index <mid:
            node.rows.insert(index,value)
        else:
            right_node.rows.insert(index-mid,value)

        #重新创建即可
        if node.is_root:
            self.create_root(node,right_node,right_node.rows[0])
            return
        #需要将节点，进行插入
        node_in_parent_index = node.parent.child_index(node)
        #不满直接插入
        insert_key = self.get_key(right_node.rows[0])
        if not len(node.parent.rows) == FULL:
            node.parent.rows.insert(node_in_parent_index+1,BranchRow(insert_key,right_node))
            return
        self.split_branch_node(node.parent, insert_key, right_node, node_in_parent_index+1)

    @staticmethod
    def   find_index_for_key_insert(keys,k):
        for i in range(len(keys)):
            if k < keys[i]:
                return i
        return len(keys)

    def could_borrow(self, node):
        """
        如果一个节点比 min_key_num 加1，就可以借
        """
        # LeafNode 有几行就有几个key
        if isinstance(node,LeafNode):
            return len(node.rows) >= self.min_key_num() + 1
        # BranNode key=rows - 1
        return len(node.rows) - 1 >= self.min_key_num() + 1

    def min_key_num(self):
        """
        节点可以拥有的最小key的数目
        """
        return FULL // 2

    def split_branch_node(self, node, key, value, key_index):
        #左边分配的长度
        mid = (FULL//2)+1


        #创建好split的节点
        right_node = BranchNode(node.parent,False)

        right_node.right = node.right
        if node.right:
            node.right.left = right_node
        right_node.left = node
        node.right = right_node

        #新增的key就是插入父级的
        if key_index == mid:
            mid_key = key
            right_node.rows = node.rows[mid:]
            right_node.rows.insert(0,BranchRow(None,value))
            node.rows = node.rows[0:mid]
        elif key_index > mid:
            mid_key = self.get_key(node.rows[mid])
            right_node.rows = node.rows[mid:]
            #特殊调整
            if len(right_node.rows) > 0:
                right_node.rows[0].key = None
            key_insert_loc = right_node.find_index_for_key_insert(key)
            right_node.rows.insert(key_insert_loc,BranchRow(key,value))
            node.rows = node.rows[0:mid]
        else:
            mid_key = self.get_key(node.rows[mid - 1])
            right_node.rows = node.rows[mid-1:]
            if len(right_node.rows) > 0:
                right_node.rows[0].key = None
            node.rows = node.rows[0:mid-1]
            key_insert_loc = node.find_index_for_key_insert(key)
            node.rows.insert(key_insert_loc,BranchRow(key,value))

        #调整右边树的结构
        for r in right_node.rows:
            r.child.parent = right_node

        #重新创建即可
        if node.is_root:
            self.create_root(node,right_node,mid_key)
            return
        #需要将节点，进行插入
        node_in_parent_index = node.parent.child_index(node)
        #不满直接插入
        if not len(node.parent.rows) == FULL:
            node.parent.rows.insert(node_in_parent_index+1,BranchRow(mid_key,right_node))
            return
        self.split_branch_node(node.parent, mid_key, right_node, node_in_parent_index+1)

    def delete(self,key):
        node = self._search(key, self.tree)

        index = node.key_index(key)
        if index == -1:
            print(f'key={key}不存在')
            return None
        #删除叶子节点的key
        result = node.rows.pop(index)

        #key的数量是rows -1
        if len(node.rows) - 1 < self.min_key_num():
            #root节点直接删除即可
            if not node.is_root:
                self.leaf_node_un_balance(node)
        return result

    def leaf_node_un_balance(self, node:LeafNode):
        """
        叶子节点不平衡
        若兄弟结点key有富余：
            向兄弟结点借一个记录，同时用借到的key替换父结（指当前结点和兄弟结点共同的父结点）点中的key，删除结束
        若兄弟结点中没有富余的key
            当前结点和兄弟结点合并成一个新的叶子结点，并删除父结点中的key（父结点中的这个key两边的孩子指针就变成了一个指针，
            正好指向这个新的叶子结点），将当前结点指向父结点（必为索引结点）

            处理索引节点

        """
        node_in_parent_index = node.parent.child_index(node)
        left_sibling:LeafNode = node.left if node.left and node.left.parent == node.parent else None
        right_sibling:LeafNode = node.right if node.right and node.right.parent == node.parent else None
        if left_sibling and self.could_borrow(left_sibling):
            row = left_sibling.rows.pop()
            node.rows.insert(0,row)
            #替换父节点中的key  这里可以取等值，和下方的处理方式不一样
            node.parent.rows[node_in_parent_index].key = self.get_key(row)
            return

        if right_sibling and self.could_borrow(right_sibling):
            row = right_sibling.rows.pop(0)
            node.rows.append(row)
            #替换父节点中的key
            # node.parent.keys[node_in_parent_index] = key
            # 这里和上方的处理不一样
            node.parent.rows[node_in_parent_index+1].key = self.get_key(right_sibling.rows[0])
            return
        if left_sibling:
            #合并到左节点
            left_sibling.rows.extend(node.rows)
            #从父节点中删除当前节点
            node.parent.rows.pop(node_in_parent_index)
            #调整左右节点
            left_sibling.right = node.right
            if node.right:
                node.right.left = left_sibling
            self.branch_node_un_balance(node.parent)
            return
        if right_sibling:
            #合并右节点到node
            node.rows.extend(right_sibling.rows)
            #从父节点中删除右节点
            node.parent.rows.pop(node_in_parent_index+1)
            #调整左右节点
            node.right = right_sibling.right
            if right_sibling.right:
                right_sibling.right.left = node
            self.branch_node_un_balance(node.parent)
            return


    def branch_node_un_balance(self,node:BranchNode):
        """
        分支节点不平衡
        若索引结点的key的个数大于等于 min_key_num结束
        若兄弟结点有富余，父结点key下移，兄弟结点key上移，删除结束
        否则：
         当前结点和兄弟结点及父结点下移key合并成一个新的结点。将当前结点指向父结点
        """
        #个数够了结束
        if len(node.rows) - 1 >= self.min_key_num():
            return
        #根节点允许一定的不平衡
        if node.is_root:
            if len(node.rows)==1:
                self.tree = node.rows[0].child
                self.tree.parent = None
                self.tree.is_root = True
            return

        parent = node.parent
        node_in_parent_index = parent.child_index(node)
        left_sibling:BranchNode = node.left if node.left and node.left.parent == parent else None
        right_sibling:BranchNode = node.right if node.right and node.right.parent == parent else None

        if left_sibling and self.could_borrow(left_sibling):
            row = left_sibling.rows.pop()
            #父节点的key下移动
            node.rows[0].key= parent.rows[node_in_parent_index].key
            #兄弟节点的key上移动,移动到父亲节点
            parent.rows[node_in_parent_index].key = row.key
            #兄弟节点移除的value给node节点 ！！！！ 需要调整 left right 关系
            node.rows.insert(0,row)
            row.key = None
            row.child.parent = node
            return
        if right_sibling and self.could_borrow(right_sibling):
            #移除 right_sibling第一个元素
            row = right_sibling.rows.pop(0)
            parent_key = parent.rows[node_in_parent_index + 1].key
            parent.rows[node_in_parent_index+1].key  = right_sibling.rows[0].key
            #设置头部的key为None
            right_sibling.rows[0].key = None
            node.rows.append(row)
            #父节点的key下移
            row.key = parent_key
            row.child.parent = node
            return

        if left_sibling:
            #合并到左节点
            #从父节点中移除 key 和node
            parent_row = parent.rows.pop(node_in_parent_index)
            node.rows[0].key = parent_row.key
            left_sibling.rows.extend(node.rows)
            for v in node.rows:
                v.child.parent = left_sibling
            left_sibling.right = node.right
            if node.right:
                node.right.left = left_sibling
            self.branch_node_un_balance(parent)
            return

        if right_sibling:
            #右节点合并到node
            parent_row = parent.rows.pop(node_in_parent_index+1)
            right_sibling.rows[0].key = parent_row.key
            node.rows.extend(right_sibling.rows)
            for v in right_sibling.rows:
                v.child.parent = node
            node.right = right_sibling.right
            if right_sibling.right:
                right_sibling.right.left = node
            self.branch_node_un_balance(parent)
            return




def del_tree(t:BTree,start,end):
    for i in range(start,end):
        t.delete(generate_row([i+1]))


def test_tree():
    t = BTree(1,False)

    for i in range(10000):
        t.insert(generate_row([i+1]))

    del_tree(t,100,150)
    del_tree(t,1000,5000)
    del_tree(t,9000,9999)
    del_tree(t,8000,8999)
    del_tree(t,6000,7777)
    del_tree(t,200,900)


    first = t.search(generate_row([1]))
    count = 0
    while first:
        count+=len(first.rows)
        print(first.rows)
        first =first.right
    print(count)


















