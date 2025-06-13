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
    def to_row(self)->Row:
        result = self.key.values.copy()
        result.append(IntValue(self.child))
        return Row(result)

def row_is_none(row:Row):
    for value in row.values:
        if not value.is_null:
            return False
    return True

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

    def is_root(self):
        return self.control_row.parent == -1

    def page_num(self):
        return self.page.page_num
    def parent(self)->int:
        return self.control_row.parent
    def set_parent(self,parent:int):
        self.control_row.parent = parent
        self.page.update_by_slot(self.control_row.to_row(),0)
    def left(self):
        return self.control_row.left
    def right(self):
        return self.control_row.right
    def set_left(self,left:int):
        self.control_row.left = left
        self.page.update_by_slot(self.control_row.to_row(),0)
    def set_right(self,right:int):
        self.control_row.right = right
        self.page.update_by_slot(self.control_row.to_row(),0)
    def get_right_node(self):
        btree:BTree = self.tree
        right_page_num = self.right()
        if right_page_num != -1:
            return btree.read_node(right_page_num)
        return None
    def get_parent_node(self):
        btree:BTree = self.tree
        parent_page_num = self.parent()
        if parent_page_num != -1:
            return btree.read_branch_node(parent_page_num)
        return None

    def get_left_node(self):
        btree:BTree = self.tree
        left_page_num = self.left()
        if left_page_num != -1:
            return btree.read_node(left_page_num)
        return None



    def move_to_another_node(self,src,target,other_node):
        other_node:Node
        self.page.move_to_another_page(src+1,target+1,other_node.page)

class LeafNode(Node):
    def __init__(self,page:CommonPage):
        super().__init__(page)

    def __repr__(self):
        return f'page_num:{self.page_num()},control_row: {self.control_row}'

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
    def child_index(self,child:int):
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            if row_i.child == child:
                return i
        return -1

    def  find_index_for_key_insert(self,k):
        # 第0个key一定是none
        for i in range(1,self.row_num()):
            row_i = self.get_row_i(i)
            if k <  row_i.key:
                return i
        return self.row_num()

    def set_child_parent(self):
        btree:BTree = self.tree
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            child = btree.read_branch_node(row_i.child)
            child.set_parent(self.page_num())

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
    def append_row(self,row:BranchRow):
        result,_ = self.page.insert_to_last_slot(row.to_row())
        if result == -1:
            raise Exception('create root fail')

    def update_row_i(self,i,value:BranchRow):
        self.page.update_by_slot(value.to_row(),i+1)

    def update_row_i_child(self,i,child:int):
        btree:BTree = self.tree
        # child是最后一个字段
        self.page.update_slot_field_by_index(i+1,btree.key_len,IntValue(child))

    def insert_row(self,i:int,value:BranchRow):
        result,_ = self.page.insert_slot(value.to_row(),i+1)
        if result == -1:
            return False
        return True
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
        if not len(record.fields) == 4:
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
        child = IntValue.from_bytes(record.fields[-1].value).value
        #读取key的内容
        key = []
        for i in range(self.key_len):
            key.append(self.value_type[i].from_bytes(record.fields[i].value))
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

    def none_key(self)->Row:
        key = []
        for i in range(self.key_len):
            key.append(self.value_type[i].none())
        return Row(key)


    def create_root(self,old_root:Node,right_node:Node,key):
        print('create root')
        old_root.is_root = False
        root = self.create_branch_node(-1)
        root.append_row(BranchRow(self.none_key(),old_root.page_num()))
        root.append_row(BranchRow(key,right_node.page_num()))
        old_root.set_parent(root.page_num())
        right_node.set_parent(root.page_num())
        self.tree = root

    def get_key(self,row)->Row:
        if isinstance(row,Row):
            return Row(row.values[0:self.key_len])
        elif isinstance(row,BranchRow):
            return row.key
        else:
            raise Exception('不支持获取key')

    def split_leaf_node(self, node:LeafNode, value, index):
        print('split leaf node')
        mid = (node.row_num() + 1)//2
        #为了分配left个节点，需要计算从node 中取的内容
        if mid > index:
            mid = mid - 1

        right_node = self.create_leaf_node(node.parent())
        right_node.set_right(node.right())
        if node.right() != -1:
            node.get_right_node().set_left(right_node.page_num())
        right_node.set_left(node.page_num())
        node.set_right(right_node.page_num())
        # node[mid:]移动到 right_node
        node.move_to_another_node(mid,node.row_num(),right_node)
        if index <mid:
            node.insert_row(index,value)
        else:
            right_node.insert_row(index-mid,value)

        #重新创建即可
        if node.is_root():
            self.create_root(node,right_node,self.get_key(right_node.get_row_i(0)))
            return
        #需要将节点，进行插入
        parent = node.get_parent_node()
        node_in_parent_index = parent.child_index(node.page_num())
        #不满直接插入
        insert_key = self.get_key(right_node.get_row_i(0))
        if parent.insert_row(node_in_parent_index+1,BranchRow(insert_key,right_node.page_num())):
            return
        self.split_branch_node(parent, insert_key, right_node, node_in_parent_index+1)

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

    def split_branch_node(self, node:BranchNode, key, value, key_index):
        if node.row_num() < 3:
            raise Exception('node key < 3')
        else:
            print('split branch')
        #左边分配的长度
        mid = (node.row_num())//2 + 1

        #创建好split的节点
        right_node = self.create_branch_node(node.parent())

        right_node.set_right(node.right())
        if node.right() != -1:
            node.get_right_node().set_left(right_node.page_num())
        right_node.set_left(node.page_num())
        node.set_right(right_node.page_num())

        #新增的key就是插入父级的
        if key_index == mid:
            mid_key = key
            node.move_to_another_node(mid,node.row_num(),right_node)
            right_node.insert_row(0,BranchRow(self.none_key(),value.page_num()))
        elif key_index > mid:
            mid_key = node.get_row_i(mid).key
            node.move_to_another_node(mid,node.row_num(),right_node)
            #特殊调整
            if right_node.row_num() > 0:
                first_row = right_node.get_row_i(0)
                first_row.key = self.none_key()
                right_node.update_row_i(0,first_row)
            key_insert_loc = right_node.find_index_for_key_insert(key)
            right_node.insert_row(key_insert_loc,BranchRow(key,value.page_num()))
        else:
            mid_key = self.get_key(node.get_row_i(mid - 1))
            node.move_to_another_node(mid-1,node.row_num(),right_node)
            if right_node.row_num() > 0:
                first_row = right_node.get_row_i(0)
                first_row.key = self.none_key()
                right_node.update_row_i(0,first_row)
            key_insert_loc = node.find_index_for_key_insert(key)
            node.insert_row(key_insert_loc,BranchRow(key,value.page_num()))


        #调整右边树的结构
        right_node.set_child_parent()
        node.set_child_parent()


        print(f'mid_key:{mid_key}')
        #重新创建即可
        if node.is_root:
            self.create_root(node,right_node,mid_key)
            return
        #需要将节点，进行插入
        parent = node.get_parent_node()
        node_in_parent_index = parent.child_index(node.page_num())
        #不满直接插入
        if parent.insert_row(node_in_parent_index+1,BranchRow(mid_key,right_node.page_num())):
            return
        self.split_branch_node(parent, mid_key, right_node, node_in_parent_index+1)

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
    t = BTree('my_tree',1,[IntValue],False)

    # t.insert(generate_row([12]))
    t.insert(generate_row([1]))
    t.insert(generate_row([2]))
    t.insert(generate_row([3]))
    t.insert(generate_row([4]))
    t.insert(generate_row([5]))
    t.insert(generate_row([6]))
    t.insert(generate_row([7]))
    t.insert(generate_row([8]))
    t.insert(generate_row([9]))
    t.insert(generate_row([10]))
    t.insert(generate_row([11]))

    print('------------insert--------------------')
    t.insert(generate_row([12]))
    node2 = t.search(generate_row([1]))
    count = 0
    while node2:
        count+=node2.row_num()
        for i in range(node2.row_num()):
            print(node2.get_row_i(i),end=',')
        print()
        node2 = node2.get_right_node()
    print(count)





test_tree()











