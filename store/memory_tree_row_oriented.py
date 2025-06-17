from typing import Tuple, List
from values import Row,generate_row

# b tree 最大容量
FULL = 9


class BranchRow:
    def __init__(self,key:Row|None,child):
        self.key = key
        self.child = child
    def __repr__(self):
        return f'key={self.key}'

class Node:
    def __init__(self,parent,is_root:bool=False):
        self.parent:Node = parent
        self.is_root = is_root
        self.left,self.right = None,None
        self.rows = None
    def key_index(self,key):
        pass
    def child_index(self,child)->int:
        pass
    def  find_index_for_key_insert(self,k)->int:
        pass
class LeafNode(Node):
    def __init__(self,parent:Node|None,is_root:bool=False):
        super().__init__(parent,is_root)
        self.rows:List[Row]=[]

    def __repr__(self):
        return f'{self.rows}'

    def key_index(self, key):
        for index,row in enumerate(self.rows):
            if key == row:
                return index
        return -1
    def  find_index_for_key_insert(self,k):
        for index,row in enumerate(self.rows):
            if k< row:
                return index
        return len(self.rows)




class BranchNode(Node):
    def __init__(self,parent:Node|None,is_root:bool=False):
        super().__init__(parent,is_root)
        self.rows:List[BranchRow]=[]

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
        return len(self.rows)


def min_key_num():
    """
    节点可以拥有的最小key的数目
    """
    return FULL // 2


class BTree:
    def __init__(self,key_len:int,duplicate_key=False):
        self.tree = LeafNode(None,True)
        self.key_len = key_len
        self.duplicate_key = duplicate_key
    def search(self,key):
        node =self._search(key,self.tree)
        return node

    def _search(self,key,node:Node,for_insert = False)->LeafNode|None:
        #叶子节点直接返回
        while not isinstance(node,LeafNode):
            branch_node:Node = node
            #branchnode 第0行数据的key是不使用的仅占位用
            i = 1
            while i < len(branch_node.rows):
                #如果允许重复插入，新节点，插入左侧节点
                cur_row_key = branch_node.rows[i].key
                if key < cur_row_key or (for_insert and key == cur_row_key and self.duplicate_key):
                    node = branch_node.rows[i-1].child
                    break
                elif key == cur_row_key:
                    node = branch_node.rows[i].child
                    break
                i+=1
            if i == len(branch_node.rows):
                node = branch_node.rows[-1].child
        return node


    def insert(self,value):
        key = self.get_key(value)
        #找到叶子节点
        node:LeafNode = self._search(key,self.tree,True)
        #替换
        index = node.key_index(key)
        if index != -1 and not self.duplicate_key:
            node.rows[index] = value
            return

        #找到一个插入的位置
        index = node.find_index_for_key_insert(key)
        #校验，插入后是否溢出
        if  len(node.rows) + 1 != FULL:
            node.rows.insert(index,value)
            return
        #溢出了，进行split,  将 FULL-1 的部分平分
        self.split_leaf_node(node, value, index)

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

        mid = FULL//2
        #为了分配left个节点，需要计算从node 中取的内容
        if mid > index:
            mid = mid - 1

        right_node = LeafNode(node.parent,False)
        right_node.right = node.right
        if node.right:
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
        if len(node.parent.rows) != FULL:
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
            return len(node.rows) >= min_key_num() + 1
        # BranNode key=rows - 1
        return len(node.rows) - 1 >= min_key_num() + 1

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
        if  len(node.parent.rows) != FULL:
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
        if len(node.rows) - 1 < min_key_num():
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
        if len(node.rows) - 1 >= min_key_num():
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


















