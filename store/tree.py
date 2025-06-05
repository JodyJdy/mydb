from typing import Tuple

FULL = 3

class Node:
    def __init__(self,parent,is_root:bool=False):
        self.keys =[]
        self.values =[]
        self.parent = parent
        self.is_root = is_root
        self.left,self.right = None,None
class LeafNode(Node):
    def __init__(self,parent:Node|None,is_root:bool=False):
        super().__init__(parent,is_root)
class BranchNode(Node):
    def __init__(self,parent:Node|None,is_root:bool=False):
        super().__init__(parent,is_root)


def update_parent_key(node:Node):
    while node.parent:
        parent = node.parent
        parent.keys[parent.values.index(node)] = node.keys[-1]
        node = parent


class BTree:
    def __init__(self,duplicate_key=False):
        self.tree = LeafNode(None,True)
        self.duplicate_key = duplicate_key
    def search(self,key):
        node =self._search(key,self.tree)
        return node

    def _search(self,key,node:Node,for_insert = False)->LeafNode|None:
        #叶子节点直接返回
        while not isinstance(node,LeafNode):
            branch_node:Node = node
            i = 0
            while i < (len(branch_node.keys)):
                #如果允许重复插入，新节点，插入左侧节点
                if key < branch_node.keys[i] or (for_insert and key == branch_node.keys[i] and self.duplicate_key):
                    node = branch_node.values[i]
                    break
                elif key == branch_node.keys[i] and i + 1 < len(branch_node.values):
                    node = branch_node.values[i+1]
                    break
                i+=1
            if i == len(branch_node.keys):
                node = branch_node.values[-1]
        return node


    def insert(self,key,value):
        #找到叶子节点
        node = self._search(key,self.tree,True)
        #替换
        if key in node.keys and not self.duplicate_key:
            index = node.keys.index(key)
            node.values[index] = value
            return

        #找到一个插入的位置
        index = BTree.find_index_for_key_insert(node.keys,key)
        #校验，插入后是否溢出
        if not len(node.keys) + 1 == FULL:
            node.keys.insert(index,key)
            node.values.insert(index,value)
            return
        #溢出了，进行split,  将 FULL-1 的部分平分
        self.split_leaf_node(node, key, value, index)

    def create_root(self,old_root,right_node,key):
        old_root.is_root = False
        root = BranchNode(None,True)
        root.keys = [key]
        root.values = [old_root,right_node]
        old_root.parent = root
        right_node.parent = root
        self.tree = root

    def split_leaf_node(self, node, key, value, index):

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
        right_node.keys = node.keys[mid:]
        right_node.values = node.values[mid:]
        #调整左边节点的数据
        node.keys = node.keys[0:mid]
        node.values = node.values[0:mid]

        if index <mid:
            node.keys.insert(index,key)
            node.values.insert(index,value)
        else:
            right_node.keys.insert(index-mid,key)
            right_node.values.insert(index-mid,value)

        #重新创建即可
        if node.is_root:
            self.create_root(node,right_node,right_node.keys[0])
            return
        #需要将节点，进行插入
        node_in_parent_index = node.parent.values.index(node)
        #不满直接插入
        if not len(node.parent.keys) + 1 == FULL:
            node.parent.keys.insert(node_in_parent_index,right_node.keys[0])
            node.parent.values.insert(node_in_parent_index+1,right_node)
            return
        self.split_branch_node(node.parent, right_node.keys[0], right_node, node_in_parent_index)

    @staticmethod
    def find_index_for_key_insert(keys,k):
        for i in range(len(keys)):
            if k < keys[i]:
                return i
        return len(keys)

    def could_borrow(self, node):
        """
        如果一个节点比 min_key_num 加1，就可以借
        """
        return len(node.keys) >= self.min_key_num() + 1

    def min_key_num(self):
        """
        节点可以拥有的最小key的数目
        """
        return FULL // 2

    def split_branch_node(self, node, key, value, key_index):
        #左边分配的长度
        mid = FULL//2

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
            right_node.keys = node.keys[mid:]
            right_node.values = node.values[mid+1:]
            right_node.values.insert(0,value)
            node.keys = node.keys[0:mid]
            node.values = node.values[0:mid+1]
        elif key_index > mid:
            mid_key = node.keys[mid]
            right_node.keys = node.keys[mid+1:] #包含 key
            right_node.values = node.values[mid+1:]
            key_insert_loc = BTree.find_index_for_key_insert(right_node.keys,key)
            right_node.keys.insert(key_insert_loc,key)
            right_node.values.insert(key_insert_loc+1,value)
            node.keys = node.keys[0:mid]
            node.values = node.values[0:mid+1]
        else:
            mid_key = node.keys[mid - 1]
            right_node.keys = node.keys[mid:]
            right_node.values = node.values[mid:]
            node.keys = node.keys[0:mid-1]
            node.values = node.values[0:mid]
            key_insert_loc = BTree.find_index_for_key_insert(node.keys,key)
            node.keys.insert(key_insert_loc,key)
            node.values.insert(key_insert_loc+1,value)

        #调整右边树的结构
        for r in right_node.values:
            r.parent = right_node

        #重新创建即可
        if node.is_root:
            self.create_root(node,right_node,mid_key)
            return
        #需要将节点，进行插入
        node_in_parent_index = node.parent.values.index(node)
        #不满直接插入
        if not len(node.parent.keys) + 1 == FULL:
            node.parent.keys.insert(node_in_parent_index,mid_key)
            node.parent.values.insert(node_in_parent_index+1,right_node)
            return
        self.split_branch_node(node.parent, mid_key, right_node, node_in_parent_index)


    def delete(self,key):
        node = self._search(key, self.tree)
        if key not in node.keys:
            print(f'key={key}不存在')
            return None
        index = node.keys.index(key)

        #有重复节点
        # if duplicate_node and isinstance(duplicate_node,BranchNode):
        #     if i + 1 < len(node.keys):
        #         duplicate_node.keys[duplicate_index] = node.keys[index + 1]
        #     elif node.right is not None:
        #         duplicate_node.keys[duplicate_index] = node.right.keys[0]
        #     else:
        #         raise Exception('异常情况')
        #删除叶子节点的key
        node.keys.pop(index)
        #删除叶子节点的value，并返回
        result = node.values.pop(index)

        #个数不够
        if len(node.keys) < self.min_key_num():
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
        node_in_parent_index = node.parent.values.index(node)
        left_sibling:LeafNode = node.left if node.left and node.left.parent == node.parent else None
        right_sibling:LeafNode = node.right if node.right and node.right.parent == node.parent else None
        if left_sibling and self.could_borrow(left_sibling):
            key = left_sibling.keys.pop()
            value = left_sibling.values.pop()
            node.keys.insert(0,key)
            node.values.insert(0,value)
            #替换父节点中的key  这里可以取等值，和下方的处理方式不一样
            node.parent.keys[node_in_parent_index - 1] = key

            return

        if right_sibling and self.could_borrow(right_sibling):
            key = right_sibling.keys.pop(0)
            value = right_sibling.values.pop(0)
            node.keys.append(key)
            node.values.append(value)
            #替换父节点中的key
            # node.parent.keys[node_in_parent_index] = key
            # 这里和上方的处理不一样
            node.parent.keys[node_in_parent_index] = right_sibling.keys[0]
            return
        if left_sibling:
            #合并到左节点
            left_sibling.keys.extend(node.keys)
            left_sibling.values.extend(node.values)
            #从父节点中删除当前节点
            node.parent.values.pop(node_in_parent_index)
            #删除key
            node.parent.keys.pop(node_in_parent_index-1)

            #调整左右节点
            left_sibling.right = node.right
            if node.right:
                node.right.left = left_sibling
            self.branch_node_un_balance(node.parent)
            return
        if right_sibling:
            #合并右节点到node
            node.keys.extend(right_sibling.keys)
            node.values.extend(right_sibling.values)
            #从父节点中删除右节点
            node.parent.values.pop(node_in_parent_index+1)
            #删除key
            node.parent.keys.pop(node_in_parent_index)
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
        if len(node.keys) >= self.min_key_num():
            return
        #根节点允许一定的不平衡
        if node.is_root:
            if len(node.keys)==0:
                self.tree = node.values[0]
                self.tree.parent = None
                self.tree.is_root = True
            return

        parent = node.parent
        node_in_parent_index = parent.values.index(node)
        left_sibling:BranchNode = node.left if node.left and node.left.parent == parent else None
        right_sibling:BranchNode = node.right if node.right and node.right.parent == parent else None

        if left_sibling and self.could_borrow(left_sibling):
            key = left_sibling.keys.pop()
            value = left_sibling.values.pop()
            #父节点的key下移动
            node.keys.insert(0,parent.keys[node_in_parent_index-1])
            #兄弟节点的key上移动,移动到父亲节点
            parent.keys[node_in_parent_index-1] = key
            #兄弟节点移除的value给node节点 ！！！！ 需要调整 left right 关系
            node.values.insert(0,value)
            value.parent = node
            return
        if right_sibling and self.could_borrow(right_sibling):
            key = right_sibling.keys.pop(0)
            value = right_sibling.values.pop(0)
            #父节点的key下移
            node.keys.append(parent.keys[node_in_parent_index])
            #兄弟节点的key上移动,移动到父亲节点
            parent.keys[node_in_parent_index] = key
            #兄弟节点移除的value给node节点  需要调整 !!!! left right 关系
            node.values.append(value)
            value.parent = node
            return

        if left_sibling:
            #合并到左节点
            #从父节点中移除 key 和node
            parent_key = parent.keys.pop(node_in_parent_index-1)
            #从父节点删除node
            parent.values.pop(node_in_parent_index)
            #父节点key
            left_sibling.keys.append(parent_key)
            left_sibling.keys.extend(node.keys)
            left_sibling.values.extend(node.values)
            for v in node.values:
                v.parent = left_sibling
            left_sibling.right = node.right
            if node.right:
                node.right.left = left_sibling
            self.branch_node_un_balance(parent)
            return

        if right_sibling:
            #右节点合并到node
            parent_key = parent.keys.pop(node_in_parent_index)
            #删除 right_sibling
            parent.values.pop(node_in_parent_index+1)
            node.keys.append(parent_key)
            node.keys.extend(right_sibling.keys)
            node.values.extend(right_sibling.values)
            for v in right_sibling.values:
                v.parent = node
            node.right = right_sibling.right
            if right_sibling.right:
                right_sibling.right.left = node
            self.branch_node_un_balance(parent)
            return








t = BTree(False)

def del_tree(t:BTree,start,end):
    for i in range(start,end):
        t.delete(i+1)


def test_tree():
    for i in range(10000):
        t.insert(i+1,i+1)


    del_tree(t,100,150)
    del_tree(t,1000,5000)
    del_tree(t,9000,9999)
    del_tree(t,8000,8999)
    del_tree(t,6000,7777)
    del_tree(t,200,900)
    first = t.search(1)
    count = 0
    while first:
        count+=len(first.values)
        print(first.values)
        first = first.right
    print(count)


















