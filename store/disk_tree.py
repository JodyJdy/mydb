import os.path
from typing import Tuple, List

import typing

import config
from store.container import Container
from store.page import Record, CommonPage, SLOT_TABLE_ENTRY_SIZE
from store.values import StrValue, BoolValue, value_type_dict
from store.values import Row, generate_row, IntValue, Value


# 叶子/分支节点都包含的信息
class ControlRow:
    def __init__(self, node_type: int = None, parent: int = None, left: int = None, right: int = None):
        self.node_type = node_type
        self.parent = parent
        self.left = left
        self.right = right

    def to_row(self):
        return generate_row([self.node_type, self.parent, self.left, self.right])

    def is_root(self):
        return self.parent == -1


class BranchRow:
    def __init__(self, key: Row, child: int):
        self.key = key
        self.child = child

    def to_row(self) -> Row:
        result = self.key.values.copy()
        result.append(IntValue(self.child))
        return Row(result)


LEAF_NODE = 0
BRANCH_NODE = 1


class Node:
    def __init__(self, page: CommonPage):
        self.page = page
        self.control_row: ControlRow | None = None
        self.tree = None

    def row_num(self):
        """
        去掉 control row
        :return:
        """
        return self.page.slot_num - 1

    def child_index(self, child) -> int:
        pass

    def find_index_for_key_insert(self, k) -> int:
        pass

    def remove_i(self, i: int):
        self.page.delete_by_slot(i + 1)

    def get_row_i(self, i: int):
        pass

    def get_last_row(self):
        pass

    def is_root(self):
        return self.control_row.parent == -1

    def page_num(self):
        return self.page.page_num

    def parent(self) -> int:
        return self.control_row.parent

    def set_parent(self, parent: int):
        self.control_row.parent = parent
        self.page.update_by_slot(self.control_row.to_row(), 0)

    def left(self):
        return self.control_row.left

    def right(self):
        return self.control_row.right

    def set_left(self, left: int):
        self.control_row.left = left
        self.page.update_by_slot(self.control_row.to_row(), 0)

    def set_right(self, right: int):
        self.control_row.right = right
        self.page.update_by_slot(self.control_row.to_row(), 0)

    def get_right_node(self):
        btree: BTree = self.tree
        right_page_num = self.right()
        if right_page_num != -1:
            return btree.read_node(right_page_num)
        return None

    def get_parent_node(self):
        btree: BTree = self.tree
        parent_page_num = self.parent()
        if parent_page_num != -1:
            return btree.read_branch_node(parent_page_num)
        return None

    def get_left_node(self):
        btree: BTree = self.tree
        left_page_num = self.left()
        if left_page_num != -1:
            return btree.read_node(left_page_num)
        return None

    def move_single_row_to_another(self, src_row_i, target_row_i, target_node):
        target_node: Node
        self.page.move_single_slot_to_another_page(src_row_i + 1, target_row_i + 1, target_node.page)

    def read_and_delete_page(self, src_slot, target_slot, page, row_list):
        btree: BTree = self.tree
        is_branch_node = self.control_row.node_type == BRANCH_NODE
        for i in range(src_slot, target_slot):
            record = page.read_slot(i)
            if is_branch_node:
                row_list.append(btree.parse_branch_row(record).to_row())
            else:
                row_list.append(btree.parse_leaf_row(record))
            page.decrease_slot_num()

    @staticmethod
    def insert_row_list_to_page(row_list, page):
        for row in row_list:
            over_flow_page: CommonPage = page.get_over_flow_page(CommonPage.record_min_size())
            over_flow_record_id, over_flow_page_num = over_flow_page.insert_to_last_slot(row)
            if over_flow_record_id == -1:
                raise Exception('插入数据失败')
            page.insert_over_flow_record(over_flow_page_num, over_flow_record_id)

    def move_to_another_node(self, src, target, other_node):
        other_node: Node
        # 在页中的slot位置
        src_slot = src + 1
        target_slot = target + 1
        cur_page = self.page
        other_page = other_node.page

        if src_slot >= cur_page.slot_num:
            return
        all_record = []
        # 计算空间
        need_space = 0
        for i in range(src_slot, target_slot):
            record_offset, record_len = cur_page.read_slot_entry(i)
            all_record.append((record_offset, record_len))
            need_space += record_len + SLOT_TABLE_ENTRY_SIZE
        # other_node不够存放，需要将other_node中除了Control Row以外的所有数据都进行over flow
        if need_space > other_page.cal_free_space():
            row_list = []
            # 读取other_page的数据
            self.read_and_delete_page(1, other_page.slot_num, other_page, row_list)
            # 读取当前 page的数据
            self.read_and_delete_page(src_slot, target_slot, cur_page, row_list)
            self.insert_row_list_to_page(row_list, other_page)
        else:
            self.page.move_to_another_page(src_slot, target_slot, other_node.page)


class LeafNode(Node):
    def __init__(self, page: CommonPage):
        super().__init__(page)

    def __repr__(self):
        return f'page_num:{self.page_num()},control_row: {self.control_row}'

    def key_index(self, key):
        # key值相等的地方
        eq_index = None
        # 符合插入条件的地方
        insert_index = None
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            if eq_index is None and key == row_i:
                eq_index = i
            if insert_index is None and key < row_i:
                insert_index = i
            if eq_index and insert_index:
                break
        if eq_index is None:
            eq_index = -1
        if insert_index is None:
            insert_index = self.row_num()
        return eq_index, insert_index

    def find_index_for_key_insert(self, key):
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            if key < row_i:
                return i
        return self.row_num()

    def get_row_i(self, i: int) -> Row:
        """
        b tree中的下标真正使用时，要 + 1， 因为 每页里面都有  control row

        :param i:
        :return:
        """
        btree: BTree = self.tree
        return btree.parse_leaf_row(self.page.read_slot(i + 1))

    def get_last_row(self):
        btree: BTree = self.tree
        return btree.parse_leaf_row(self.page.read_slot(self.page.slot_num - 1))

    def update_row_i(self, i, value: Row):
        self.page.update_by_slot(value, i + 1)

    def insert_row(self, i: int, value: Row):
        result, _ = self.page.insert_slot(value, i + 1)
        if result == -1:
            return False
        return True


class BranchNode(Node):
    def __init__(self, page: CommonPage):
        super().__init__(page)

    def child_index(self, child: int):
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            if row_i.child == child:
                return i
        return -1

    def find_index_for_key_insert(self, k):
        # 第0个key一定是none
        for i in range(1, self.row_num()):
            row_i = self.get_row_i(i)
            if k < row_i.key:
                return i
        return self.row_num()

    def set_child_parent(self, page_num: int = None):
        if page_num is None:
            page_num = self.page_num()
        btree: BTree = self.tree
        for i in range(self.row_num()):
            row_i = self.get_row_i(i)
            child = btree.read_branch_node(row_i.child)
            child.set_parent(page_num)

    def get_row_i(self, i: int) -> BranchRow:
        """
        b tree中的下标真正使用时，要 + 1， 因为 每页里面都有  control row

        :param i:
        :return:
        """
        btree: BTree = self.tree
        return btree.parse_branch_row(self.page.read_slot(i + 1))

    def get_last_row(self):
        btree: BTree = self.tree
        return btree.parse_branch_row(self.page.read_slot(self.page.slot_num - 1))

    def append_row(self, row: BranchRow):
        result, _ = self.page.insert_to_last_slot(row.to_row())
        if result == -1:
            raise Exception('insert b tree error')

    def update_row_i(self, i, value: BranchRow):
        self.page.update_by_slot(value.to_row(), i + 1)

    def update_row_i_child(self, i, child: int):
        btree: BTree = self.tree
        # child是最后一个字段
        self.page.update_slot_field_by_index(i + 1, btree.key_len, IntValue(child))

    def get_row_i(self, i: int) -> BranchRow:
        """
        b tree中的下标真正使用时，要 + 1， 因为 每页里面都有  control row

        :param i:
        :return:
        """
        btree: BTree = self.tree
        return btree.parse_branch_row(self.page.read_slot(i + 1))

    def get_last_row(self) -> BranchRow:
        btree: BTree = self.tree
        return btree.parse_branch_row(self.page.read_slot(self.page.slot_num - 1))

    def insert_row(self, i: int, value: BranchRow):
        result, _ = self.page.insert_slot(value.to_row(), i + 1)
        if result == -1:
            # 一个 branch row 最少也要有三条记录，才能够进行 split branch操作
            if self.row_num() < 3:
                # 插入失败，需要调整空间，将 page的内容全部over flow
                row_list = []
                self.read_and_delete_page(1, self.page.slot_num, self.page, row_list)
                row_list.insert(i, value.to_row())
                self.insert_row_list_to_page(row_list, self.page)
                return True
            return False
        return True

class BTreeInfo:
    def __init__(self,name: str,root:int, key_len: int,  duplicate_key:bool,value_types: List[typing.Type[Value]]):
        self.name:str = name
        self.key_len:int = key_len
        self.duplicate_key:bool = duplicate_key
        self.root = root
        self.value_types:List[typing.Type[Value]] = value_types

    def to_row(self)->Row:
        row_list = [StrValue(self.name),IntValue(self.key_len),BoolValue(self.duplicate_key),
                    IntValue(self.root)
                    ]
        for value_type in self.value_types:
            row_list.append(IntValue(value_type.type_enum()))
        return Row(row_list)
    @staticmethod
    def parse_record(record:Record):
        name = StrValue.from_bytes(record.fields[0].value).value
        key_len = IntValue.from_bytes(record.fields[1].value).value
        duplicate_key = BoolValue.from_bytes(record.fields[2].value).value
        root = IntValue.from_bytes(record.fields[3].value).value
        value_types: List[typing.Type[Value]] = []
        for field in record.fields[4:]:
            value_types.append(value_type_dict[IntValue.from_bytes(field.value).value])
        return BTreeInfo(name,root,key_len,duplicate_key,value_types)

class BTree:

    @staticmethod
    def create_btree(btree: BTreeInfo,if_not_exist:bool = False):
        if config.container_exists(btree.name) and not if_not_exist:
            raise Exception(f'btree {btree.name} already exists')
        #记录
        config.add_container(btree.name)
        #page 1 总是存放 btree的信息
        container = Container(btree.name)
        btree_info_page = container.new_common_page(is_over_flow=False)
        #创建根节点
        root_page = container.new_common_page()
        control_row = ControlRow(LEAF_NODE, -1, -1, -1)
        root_page.insert_to_last_slot(control_row.to_row())
        #记录节点信息
        btree.root = root_page.page_num
        btree_info_page.insert_to_last_slot(btree.to_row())
        container.flush()
        container.close()

    @staticmethod
    def open_btree(name:str):
        if not config.container_exists(name):
            raise Exception(f'btree {name} not exists')
        container = Container(name)
        #page 1 总是存放 btree的信息, page 0 是container的页码管理页
        btree_info_page = container.get_page(config.BTREE_INFO_PAGE_NUM)
        btree_info = BTreeInfo.parse_record(btree_info_page.read_slot(0))
        return BTree(btree_info)


    def __init__(self, btree_info:BTreeInfo):
        self.container = Container(btree_info.name)
        self.tree = self.read_node(btree_info.root)
        self.key_len = btree_info.key_len
        self.value_type = btree_info.value_types
        self.duplicate_key = btree_info.duplicate_key

    def create_leaf_node(self, parent: int):
        page = self.container.new_common_page()
        leaf = LeafNode(page)
        leaf.control_row = ControlRow(LEAF_NODE, parent, -1, -1)
        page.insert_to_last_slot(leaf.control_row.to_row())
        leaf.tree = self
        return leaf

    def read_node(self, page_num: int) -> LeafNode:
        page = self.container.get_page(page_num)
        control_row = self.parse_control_row(page.read_slot(0))
        if control_row.node_type == LEAF_NODE:
            node = LeafNode(page)
        else:
            node = BranchNode(page)
        node.control_row = self.parse_control_row(page.read_slot(0))
        node.tree = self
        return node

    def read_branch_node(self, page_num: int) -> BranchNode:
        page = self.container.get_page(page_num)
        branch = BranchNode(page)
        branch.control_row = self.parse_control_row(page.read_slot(0))
        branch.tree = self
        return branch

    def create_branch_node(self, parent: int):
        page = self.container.new_common_page()
        branch = BranchNode(page)
        branch.control_row = ControlRow(BRANCH_NODE, parent, -1, -1)
        page.insert_to_last_slot(branch.control_row.to_row())
        branch.tree = self
        return branch

    def parse_control_row(self, record: Record):
        if not len(record.fields) == 4:
            raise Exception(f'record内容与数据类型不匹配')
        node_type = IntValue.from_bytes(record.fields[0].value).value
        parent = IntValue.from_bytes(record.fields[1].value).value
        left = IntValue.from_bytes(record.fields[2].value).value
        right = IntValue.from_bytes(record.fields[3].value).value
        return ControlRow(node_type=node_type, parent=parent, left=left, right=right)

    def parse_branch_row(self, record: Record):
        if not len(record.fields) == self.key_len + 1:
            raise Exception(f'record内容与数据类型不匹配')
        # 第一位是 child
        child = IntValue.from_bytes(record.fields[-1].value).value
        # 读取key的内容
        key = []
        for i in range(self.key_len):
            if record.fields[i].is_null():
                key.append(self.value_type[i].none())
            else:
                key.append(self.value_type[i].from_bytes(record.fields[i].value))
        return BranchRow(Row(key), child)

    def parse_leaf_row(self, record: Record):
        if not len(record.fields) == len(self.value_type):
            raise Exception(f'record内容与数据类型不匹配')
        result = []
        for value_type, field in zip(self.value_type, record.fields):
            if field.is_null():
                result.append(value_type.none())
            else:
                result.append(value_type.from_bytes(field.value))
        return Row(result)

    def search(self, key):
        node = self._search(key, self.tree)
        return node

    def _search(self, key, node, for_insert=False) -> LeafNode | None:
        # 叶子节点直接返回
        while not isinstance(node, LeafNode):
            branch_node: BranchNode = node
            # branchnode 第0行数据的key是不使用的仅占位用
            i = 1
            row_num = branch_node.row_num()
            if row_num < 1:
                raise Exception(f'B tree 结构错误:page:{branch_node.page_num()}')
            while i < row_num:
                # 如果允许重复插入，新节点，插入左侧节点
                cur_row_key = branch_node.get_row_i(i).key
                if key < cur_row_key or (for_insert and key == cur_row_key and self.duplicate_key):
                    node = self.read_node(branch_node.get_row_i(i - 1).child)
                    break
                elif key == cur_row_key:
                    node = self.read_node(branch_node.get_row_i(i).child)
                    break
                i += 1
            if i == row_num:
                node = self.read_node(branch_node.get_last_row().child)
        return node

    def update(self,value):
        key = self.get_key(value)
        # 找到叶子节点
        node: LeafNode = self._search(key, self.tree, True)
        # 查找key相等的部分，如果不存在，就找可以插入的部分
        eq_index, _ = node.key_index(key)
        if eq_index == -1:
            raise Exception(f'key={key}不存在')
        node.update_row_i(eq_index,value)

    def insert(self, value):
        key = self.get_key(value)
        # 找到叶子节点
        node: LeafNode = self._search(key, self.tree, True)
        # 查找key相等的部分，如果不存在，就找可以插入的部分
        eq_index, insert_index = node.key_index(key)
        if eq_index != -1 and not self.duplicate_key:
            node.update_row_i(eq_index, value)
            return
        # 校验，直接进入插入，如果插入失败，说明满了
        if node.insert_row(insert_index, value):
            return
        # 溢出了，进行split,  将 FULL-1 的部分平分
        self.split_leaf_node(node, value, insert_index)

    def none_key(self) -> Row:
        key = []
        for i in range(self.key_len):
            key.append(self.value_type[i].none())
        return Row(key)

    def update_root(self):
        #btree 信息放在第一页
        btree_info_page = self.container.get_page(config.BTREE_INFO_PAGE_NUM)
        btree_info_page.update_field_by_index(0,3,IntValue(self.tree.page_num()))
        btree_info_page.flush()

    def create_root(self, old_root: Node, right_node: Node, key):
        root = self.create_branch_node(-1)
        root.append_row(BranchRow(self.none_key(), old_root.page_num()))
        root.append_row(BranchRow(key, right_node.page_num()))
        old_root.set_parent(root.page_num())
        right_node.set_parent(root.page_num())
        self.tree = root
        #更新root节点
        self.update_root()

    def get_key(self, row) -> Row:
        if isinstance(row, Row):
            return Row(row.values[0:self.key_len])
        elif isinstance(row, BranchRow):
            return row.key
        else:
            raise Exception('不支持获取key')

    def split_leaf_node(self, node: LeafNode, value, index):
        mid = (node.row_num() + 1) // 2
        # 为了分配left个节点，需要计算从node 中取的内容
        if mid > index:
            mid = mid - 1

        right_node = self.create_leaf_node(node.parent())
        right_node.set_right(node.right())
        if node.right() != -1:
            node.get_right_node().set_left(right_node.page_num())
        right_node.set_left(node.page_num())
        node.set_right(right_node.page_num())
        # node[mid:]移动到 right_node
        node.move_to_another_node(mid, node.row_num(), right_node)
        if index < mid:
            node.insert_row(index, value)
        else:
            right_node.insert_row(index - mid, value)

        # 重新创建即可
        if node.is_root():
            self.create_root(node, right_node, self.get_key(right_node.get_row_i(0)))
            return
        # 需要将节点，进行插入
        parent = node.get_parent_node()
        node_in_parent_index = parent.child_index(node.page_num())
        # 不满直接插入
        insert_key = self.get_key(right_node.get_row_i(0))
        if parent.insert_row(node_in_parent_index + 1, BranchRow(insert_key, right_node.page_num())):
            return
        self.split_branch_node(parent, insert_key, right_node, node_in_parent_index + 1)

    @staticmethod
    def find_index_for_key_insert(keys, k):
        for i in range(len(keys)):
            if k < keys[i]:
                return i
        return len(keys)

    def split_branch_node(self, node: BranchNode, key, value, key_index):
        if node.row_num() < 3:
            raise Exception('node key < 3')
        # 左边分配的长度
        mid = (node.row_num()) // 2 + 1

        # 创建好split的节点
        right_node = self.create_branch_node(node.parent())

        right_node.set_right(node.right())
        if node.right() != -1:
            node.get_right_node().set_left(right_node.page_num())
        right_node.set_left(node.page_num())
        node.set_right(right_node.page_num())

        # 新增的key就是插入父级的
        if key_index == mid:
            mid_key = key
            node.move_to_another_node(mid, node.row_num(), right_node)
            if not right_node.insert_row(0, BranchRow(self.none_key(), value.page_num())):
                raise Exception('插入失败')

        elif key_index > mid:
            mid_key = node.get_row_i(mid).key
            node.move_to_another_node(mid, node.row_num(), right_node)
            # 特殊调整
            if right_node.row_num() > 0:
                first_row = right_node.get_row_i(0)
                first_row.key = self.none_key()
                right_node.update_row_i(0, first_row)
            key_insert_loc = right_node.find_index_for_key_insert(key)
            if not right_node.insert_row(key_insert_loc, BranchRow(key, value.page_num())):
                raise Exception('插入失败')
        else:
            mid_key = self.get_key(node.get_row_i(mid - 1))
            node.move_to_another_node(mid - 1, node.row_num(), right_node)
            if right_node.row_num() > 0:
                first_row = right_node.get_row_i(0)
                first_row.key = self.none_key()
                right_node.update_row_i(0, first_row)
            key_insert_loc = node.find_index_for_key_insert(key)
            if not node.insert_row(key_insert_loc, BranchRow(key, value.page_num())):
                raise Exception('插入失败')

        # 调整右边树的结构
        right_node.set_child_parent()
        node.set_child_parent()

        # 重新创建即可
        if node.is_root():
            self.create_root(node, right_node, mid_key)
            return
        # 需要将节点，进行插入
        parent = node.get_parent_node()
        node_in_parent_index = parent.child_index(node.page_num())
        # 不满直接插入
        if parent.insert_row(node_in_parent_index + 1, BranchRow(mid_key, right_node.page_num())):
            return
        self.split_branch_node(parent, mid_key, right_node, node_in_parent_index + 1)

    def delete(self, key):
        node = self._search(key, self.tree)

        index, _ = node.key_index(key)
        if index == -1:
            print(f'key={key}不存在')
            return False
        # 删除叶子节点的key
        node.remove_i(index)
        # 对于磁盘上的 B tree 当 一个节点都没有时，才考虑balance操作
        if node.row_num() <= 1:
            # root节点直接删除即可
            if not node.is_root():
                self.leaf_node_un_balance(node)
        return True

    def leaf_node_un_balance(self, node: LeafNode):
        """
        叶子节点不平衡
        若兄弟结点key有富余：
            向兄弟结点借一个记录，同时用借到的key替换父结（指当前结点和兄弟结点共同的父结点）点中的key，删除结束
        若兄弟结点中没有富余的key
            当前结点和兄弟结点合并成一个新的叶子结点，并删除父结点中的key（父结点中的这个key两边的孩子指针就变成了一个指针，
            正好指向这个新的叶子结点），将当前结点指向父结点（必为索引结点）

            处理索引节点

        """
        parent = node.get_parent_node()
        node_in_parent_index = parent.child_index(node.page_num())
        left = node.get_left_node()
        right = node.get_right_node()
        left_sibling, right_sibling = None, None
        if left and left.parent() == node.parent():
            left_sibling = left
        if right and right.parent() == node.parent():
            right_sibling = right

        # 起码要有两个才能借
        if left_sibling and left_sibling.row_num() >= 2:
            row = left_sibling.get_last_row()
            left_sibling.move_single_row_to_another(left_sibling.row_num() - 1, 0, node)
            # 替换父节点中的key  这里可以取等值，和下方的处理方式不一样
            parent_row: BranchRow = parent.get_row_i(node_in_parent_index)
            parent_row.key = self.get_key(row)
            parent.update_row_i(node_in_parent_index, parent_row)
            return

        if right_sibling and right_sibling.row_num() >= 2:
            right_sibling.move_single_row_to_another(0, node.row_num(), node)
            # 替换父节点中的key
            parent_row: BranchRow = parent.get_row_i(node_in_parent_index + 1)
            parent_row.key = self.get_key(right_sibling.get_row_i(0))
            parent.update_row_i(node_in_parent_index + 1, parent_row)
            return

        if left_sibling:
            # 合并到左节点
            node.move_to_another_node(0, node.row_num(), left_sibling)
            # 从父节点中删除当前节点
            parent.remove_i(node_in_parent_index)
            # 调整左右节点
            left_sibling.set_right(node.right())
            if node.right() != -1:
                node.get_right_node().set_left(left_sibling.page_num())
            self.branch_node_un_balance(parent)
            # 释放 node 所在的page
            self.container.free_page(node.page_num())
            return
        if right_sibling:
            # 合并右节点到node
            right_sibling.move_to_another_node(0, right_sibling.row_num(), node)
            # 从父节点中删除右节点
            parent.remove_i(node_in_parent_index + 1)
            # 调整左右节点
            node.set_right(right_sibling.right())
            if right_sibling.right() != -1:
                right_sibling.get_right_node().set_left(node.page_num())
            self.branch_node_un_balance(parent)
            # 释放 right_sibling 所在的page
            self.container.free_page(right_sibling.page_num())
            return

        raise Exception("B tree error")

    def branch_node_un_balance(self, node: BranchNode):
        """
        分支节点不平衡
        若索引结点的key的个数大于等于 min_key_num结束
        若兄弟结点有富余，父结点key下移，兄弟结点key上移，删除结束
        否则：
         当前结点和兄弟结点及父结点下移key合并成一个新的结点。将当前结点指向父结点
        """
        # 只要有一个就结束
        if node.row_num() >= 2:
            return
        # 根节点允许一定的不平衡
        if node.is_root():
            if node.row_num() == 1:
                self.tree = self.read_node(node.get_row_i(0).child)
                self.tree.set_parent(-1)
                self.tree.set_left(-1)
                self.tree.set_right(-1)
                self.update_root()
                return

        parent = node.get_parent_node()
        node_in_parent_index = parent.child_index(node.page_num())
        left = node.get_left_node()
        right = node.get_right_node()
        left_sibling, right_sibling = None, None
        if left and left.parent() == node.parent():
            left_sibling = left
        if right and right.parent() == node.parent():
            right_sibling = right

        if left_sibling and left_sibling.row_num() >= 3:
            left_sibling: BranchNode
            row: BranchRow = left_sibling.get_last_row()
            left_sibling.remove_i(left_sibling.row_num() - 1)
            parent_row = parent.get_row_i(node_in_parent_index)
            node_first = node.get_row_i(0)
            node_first.key = parent_row.key
            parent_row.key = row.key
            # 父节点的key下移
            node.update_row_i(0, node_first)
            parent.update_row_i(node_in_parent_index, parent_row)
            # 兄弟节点的key上移动,移动到父亲节点
            # 兄弟节点移除的value给node节点 ！！！！ 需要调整 left right 关系
            row.key = self.none_key()
            self.read_node(row.child).set_parent(node.page_num())
            node.insert_row(0, row)
            return
        if right_sibling and right_sibling.row_num() >= 3:
            right_sibling: BranchNode
            # 移除 right_sibling第一个元素
            row = right_sibling.get_row_i(0)
            right_sibling.remove_i(0)
            parent_row = parent.get_row_i(node_in_parent_index + 1)
            parent_key = parent_row.key
            right_first = right_sibling.get_row_i(0)
            parent_row.key = right_first.key
            parent.update_row_i(node_in_parent_index + 1, parent_row)
            right_first.key = self.none_key()
            right_sibling.update_row_i(0, right_first)
            row.key = parent_key
            self.read_node(row.child).set_parent(node.page_num())
            node.append_row(row)
            return

        if left_sibling:
            # 合并到左节点
            # 从父节点中移除 key 和node
            parent_row = parent.get_row_i(node_in_parent_index)
            parent.remove_i(node_in_parent_index)

            node_first = node.get_row_i(0)
            node_first.key = parent_row.key
            node.update_row_i(0, node_first)
            node.set_child_parent(left_sibling.page_num())
            node.move_to_another_node(0, node.row_num(), left_sibling)
            left_sibling.set_right(node.right())
            if node.right() != -1:
                node.get_right_node().set_left(left_sibling.page_num())
            self.branch_node_un_balance(parent)
            # 释放 node 所在的page
            self.container.free_page(node.page_num())
            return

        if right_sibling:
            # 右节点合并到node
            parent_row = parent.get_row_i(node_in_parent_index + 1)
            parent.remove_i(node_in_parent_index + 1)
            right_sibling_first = right_sibling.get_row_i(0)
            right_sibling_first.key = parent_row.key
            right_sibling.update_row_i(0, right_sibling_first)

            right_sibling.set_child_parent(node.page_num())
            right_sibling.move_to_another_node(0, right_sibling.row_num(), node)
            node.set_right(right_sibling.right())
            if right_sibling.right() != -1:
                right_sibling.get_right_node().set_left(node.page_num())
            self.branch_node_un_balance(parent)
            # 释放 right_sibling 所在的page
            self.container.free_page(right_sibling.page_num())
            return
        raise Exception("B tree error")

    def show(self):
        q = [self.tree]
        while len(q) > 0:
            node = q.pop(0)
            num = node.row_num()
            if isinstance(node, BranchNode):
                keys = []
                child = []
                print_child = []
                node_type = None
                child_page = []
                for i in range(num):
                    row = node.get_row_i(i)
                    child_page.append(row.child)
                    if row.key != self.none_key():
                        keys.append(row.key)
                    child_node = self.read_node(row.child)
                    if isinstance(child_node, BranchNode):
                        node_type = 'BranchNode'
                    else:
                        node_type = 'LeafNode'
                    child.append(child_node)
                    temp_child = []
                    for x in range(child_node.row_num()):
                        temp_row = child_node.get_row_i(x)
                        if isinstance(temp_row, Row):
                            temp_child.append(temp_row)
                        elif isinstance(temp_row, BranchRow):
                            if temp_row.key != self.none_key():
                                temp_child.append(temp_row.key)
                    print_child.append(temp_child)
                print(
                    f'page_num:{node.page_num()} , keys ={keys}, {node_type}child ={print_child},child_page={child_page}')
                q.extend(child)
            elif isinstance(node, LeafNode):
                pass
