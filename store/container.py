import mmap

import file_util
import os
import struct
from typing import Dict, Tuple

from store.values import OverFlowValue, Row


class BasePage:
    OVER_FLOW_PAGE = 1

    def __init__(self, page_num: int, page_data: bytearray):
        self.page_num = page_num
        self.page_data = page_data

    def sync(self):
        """
        状态同步到 page_data中
        :return:
        """
        pass

    def header_size(self) -> int:
        pass

    def insert_slot(self, row: Row, slot: int):
        pass

    def init_page(self):
        """新创建页面时，需要初始化"""
        pass


class Extent:
    EXTENT_UN_FILL = 0
    EXTENT_FILL = 1
    PAGE_NOT_USE = 0
    PAGE_USED = 1
    INVALID_PAGE_NUMBER = -1

    def __init__(self, first: int, last: int, length: int, status, free_status: bytearray, dirty: bool = False):
        self.first = first
        self.last = last
        self.length = length
        self.status = status
        self.free_status = free_status
        self.dirty = dirty

    def has_new_page(self):
        return not self.last == self.first + self.length

    def free_page(self, page_num: int):
        self.set_page_status(page_num, Extent.PAGE_NOT_USE)

    def set_page_status(self, page_num: int, status: int):
        self.free_status[page_num - self.first] = status

    def alloc_page(self) -> int:
        if self.status == Extent.EXTENT_FILL:
            return Extent.INVALID_PAGE_NUMBER
        # 检查有无空闲
        i = 0
        while i < self.last:
            if self.free_status[i] == Extent.PAGE_NOT_USE:
                self.set_page_status(i + self.first, Extent.PAGE_USED)
                return i
            i += 1
        # 重新创建
        page_num = self.alloc_new_page()
        if page_num == Extent.INVALID_PAGE_NUMBER:
            self.status = Extent.EXTENT_FILL
        return Extent.INVALID_PAGE_NUMBER

    def alloc_new_page(self) -> int:
        if self.has_new_page():
            self.dirty = True
            page_num = self.last
            self.set_page_status(page_num, Extent.PAGE_USED)
            self.last += 1
            return page_num
        return Extent.INVALID_PAGE_NUMBER


class ContainerAlloc:
    """
    alloc结构:
      4 byte   extent 数量
      <num> extent1 extent2....
    extent结构，大小固定
    first  首个page number  4
    last   上个分配的 page number 4
    length  可以存储的page number 4
    status  状态 4
    管理页面的状态
    """
    # 每个extent管理的页的数量
    PER_EXTENT_PAGE_NUM = 1024
    # 仅存储extent数量
    HEADER_SIZE = 4
    EXTENT_HEADER_SIZE = 4 + 4 + 4 + 4
    INVALID_EXTENT_NUM = -1

    def __init__(self, container_path: str):
        self.extent_dict: Dict[int, Extent] = {}
        self.path = container_path + '.alloc'
        file_util.create_file_if_need(self.path)
        self.file = open(self.path, 'rb+')
        self.dirty = False
        # 未初始化，进行初始化
        if self.get_size() < self.HEADER_SIZE:
            self.file.seek(0)
            self.file.write(struct.pack('<i', ContainerAlloc.INVALID_EXTENT_NUM))
            self.file.flush()
            self.dirty = True
        self.file.seek(0)
        self.extent_num = struct.unpack('<i', self.file.read(ContainerAlloc.HEADER_SIZE))[0]

    def add_extent(self):
        self.dirty = True
        self.extent_num += 1
        self.extent_dict[self.extent_num] = self.create_extent(self.extent_num)

    def alloc_new(self) -> int:
        if self.extent_num == ContainerAlloc.INVALID_EXTENT_NUM:
            self.add_extent()
        while True:
            extent = self.get_extent(self.extent_num)
            if not extent.has_new_page():
                self.add_extent()
            else:
                return extent.alloc_new_page()

    def get_extent(self, i: int):
        if i in self.extent_dict:
            return self.extent_dict[i]
        extent = self.read_extent(i)
        self.extent_dict[i] = extent
        return extent

    def free_page(self, page_num: int):
        extent = page_num // ContainerAlloc.PER_EXTENT_PAGE_NUM
        # 不存在
        if extent > self.extent_num:
            return
        self.get_extent(extent).free_page(page_num)

    def create_extent(self, i: int) -> Extent:
        first = last = i * ContainerAlloc.PER_EXTENT_PAGE_NUM
        length = self.PER_EXTENT_PAGE_NUM
        status = Extent.EXTENT_UN_FILL
        free_status = bytearray(ContainerAlloc.PER_EXTENT_PAGE_NUM)
        offset = i * ContainerAlloc.extent_space_size() + ContainerAlloc.HEADER_SIZE
        self.file.seek(offset)
        self.file.write(struct.pack('<iiii', first, last, length, status))
        self.file.write(free_status)
        return Extent(first, last, length, status, free_status, False)

    @staticmethod
    def extent_space_size() -> int:
        """一个extent占据空间的大小"""
        return ContainerAlloc.EXTENT_HEADER_SIZE + ContainerAlloc.PER_EXTENT_PAGE_NUM

    def read_extent(self, i: int):
        # 读取extent偏移量
        offset = i * ContainerAlloc.extent_space_size() + ContainerAlloc.HEADER_SIZE
        self.file.seek(offset)
        free_status = bytearray()
        free_status.extend(self.file.read(ContainerAlloc.extent_space_size()))
        first, last, length, status = struct.unpack('<iiii', free_status[0:ContainerAlloc.EXTENT_HEADER_SIZE])
        return Extent(first, last, length, status, free_status[ContainerAlloc.EXTENT_HEADER_SIZE:])

    def alloc(self, new_page: bool = False):
        if new_page:
            return self.alloc_new()
        """
        可以复用之前的页
        :return:
        """
        if self.extent_num == ContainerAlloc.INVALID_EXTENT_NUM:
            self.add_extent()
        i = 0
        while i <= self.extent_num:
            extent = self.get_extent(self.extent_num)
            page_num = extent.alloc_page()
            if not page_num == Extent.INVALID_PAGE_NUMBER:
                return page_num
            i = i + 1
        # 无空闲页
        return self.alloc_new()

    def get_size(self):
        return os.path.getsize(self.path)

    def flush(self):
        if self.dirty:
            self.file.seek(0)
            content = struct.pack('<i', self.extent_num)
            self.file.write(content)
        for i, e in self.extent_dict.items():
            if e.dirty:
                offset = i * ContainerAlloc.extent_space_size() + ContainerAlloc.HEADER_SIZE
                self.file.seek(offset)
                self.file.write(struct.pack('<iiii', e.first, e.last, e.length, e.status))
                self.file.write(e.free_status)
        self.file.flush()

    def close(self):
        self.file.close()


class Container:
    # 页的大小
    PAGE_SIZE = 1024

    def __init__(self, path: str | None = None):
        self.path = path
        file_util.create_file_if_need(self.path)
        self.alloc = ContainerAlloc(self.path)
        self.file = open(self.path, 'rb+')

    def get_size(self):
        return os.path.getsize(self.path)

    def seek_page(self, page_number: int):
        offset = page_number * Container.PAGE_SIZE
        self.file.seek(offset)

    def seek_offset(self, offset: int):
        self.file.seek(offset)

    def read_page(self, page_number: int, page_data: bytearray):
        self.seek_page(page_number)
        page_data.extend(self.file.read(Container.PAGE_SIZE))

    def pad_file(self, offset: int):
        cur_eof = self.get_size()
        self.file.seek(cur_eof)
        zero_data = bytearray(Container.PAGE_SIZE)
        while cur_eof < offset:
            diff = offset - cur_eof
            if diff > Container.PAGE_SIZE:
                diff = Container.PAGE_SIZE
                self.file.write(zero_data[:diff])
            cur_eof = cur_eof + diff

    def write_page(self, page_number: int, page_data: bytearray):
        offset = page_number * Container.PAGE_SIZE
        self.seek_offset(offset)
        if not self.get_size() == offset:
            self.pad_file(offset)
        self.file.write(page_data)

    def add_page(self):
        pass

    def init_page(self):
        pass

    def new_page(self) -> BasePage:
        """
        新创建需要初始化
        :return:
        """
        page_data = bytearray(Container.PAGE_SIZE)
        page_num = self.alloc.alloc()
        page = OverFlowPage(page_num, page_data)
        page.init_page()
        return page

    def close(self):
        self.alloc.close()
        self.file.close()

    def flush(self):
        self.alloc.flush()
        self.file.flush()


class OverFlowPage(BasePage):
    SINGLE_PAGE = 1
    MULTI_PAGE = 0
    """
    结构:
    是否是 over_flow_page
     slot数量  4
     下一个可用的记录id  4
     record1,record2,record3 ....
     <slot table>

     record结构:
     status 是否在当页存储完 1
     record id 当前记录的id  4
     length  当页存储长度 4
     next_page_num  如果没有存储完， 放置的下一个页  4
     next_page_record_id   放置页的record_id  4

     <slot table>
      slot 偏移量  4
      slot 长度
    """

    def __init__(self, page_num: int, page_data: bytearray):
        super().__init__(page_num, page_data)
        self.page_type, self.slot_num, self.next_id = struct.unpack_from('<bii', self.page_data, 0)

    def set_container(self, container: Container):
        self.container = container

    @staticmethod
    def cal_slot_entry_offset(slot: int):
        return Container.PAGE_SIZE - (slot + 1) * OverFlowPage.slot_table_entry_size()

    def read_slot_entry(self, slot) -> Tuple[int, int]:
        """
        返回 记录的: offset, length
        :param slot:
        :return:
        """
        offset = self.cal_slot_entry_offset(slot)
        return struct.unpack_from('<ii', self.page_data, offset)

    @staticmethod
    def slot_table_entry_size() -> int:
        return 4 + 4

    @staticmethod
    def record_header_size() -> int:
        """记录头大小"""
        return 1 + 4 + 4 + 4 + 4

    @staticmethod
    def record_min_size() -> int:
        """
        存储一个记录最少需要的空间
        :return:
        """
        return OverFlowPage.record_header_size() + OverFlowPage.slot_table_entry_size()

    def cal_free_space(self):
        space_used = self.header_size() + self.slot_num * OverFlowPage.slot_table_entry_size()
        for i in range(self.slot_num):
            record_offset, record_len = self.read_slot_entry(i)
            space_used += record_len
        return Container.PAGE_SIZE - space_used

    def read_slot(self, slot: int) -> bytearray|None:
        if slot >= self.slot_num:
            return None
        offset, length = self.read_slot_entry(slot)
        return self.page_data[offset:offset + length]

    def insert_slot(self, row: Row, slot: int):
        if not isinstance(row.values[0], OverFlowValue):
            raise TypeError('over_flow_page中只能插入OverFlowValue')
        v: OverFlowValue = row.values[0]
        cur_page = self
        neet_space = v.space_use() + self.record_min_size()
        # data_length是record中单纯数据的长度
        status, data_length, next_page_num, next_record_id = OverFlowPage.SINGLE_PAGE, 0, -1, -1
        free_space = self.cal_free_space()
        # 全部都可以放下
        if neet_space <= free_space:
            data_length = v.space_use()
        else:
            # 能放多少放多少
            status = OverFlowPage.MULTI_PAGE
            data_length = free_space - self.record_min_size()
        # 连头部信息，slot_table_entry_size 都不能存放
        if data_length < 0:
            return -1
        # 进行存放
        record_id = self.get_next_record_id()

        # 记录存储时真正占用的空间（不包含slot entry)
        record_length = data_length + self.record_header_size()
        record_start, slot_start = self.insert_shift(slot, record_length)
        # 写入头部
        struct.pack_into('<biiii', self.page_data, record_start, status, record_id, data_length, next_page_num,
                         next_record_id)
        # 写入数据部分
        self.page_data[record_start + self.record_header_size():record_start + self.record_header_size() + data_length] \
            = v.value[0:data_length]
        # 写入 slot table
        struct.pack_into('<ii', self.page_data, slot_start, record_start, record_length)
        # 返回插入记录的页面id
        self.slot_num += 1
        return record_id

    def cal_record_offset(self, slot: int):
        if slot == 0:
            return self.header_size()
        offset, _ = self.read_slot_entry(slot)
        return offset

    def position_for_append_record(self):
        """
        可以用于追加记录的位置
        :return:
        """
        if self.slot_num == 0:
            return self.header_size()
        # 读取最新一条，slot
        offset, length = self.read_slot_entry(self.slot_num - 1)
        return offset + length

    def delete_shift(self, slot: int) -> bool:
        if slot >= self.slot_num:
            return False
        # 删除最后一个，使用填充0的方式
        if slot == self.slot_num - 1:
            struct.pack_into('<ii', self.page_data, Container.PAGE_SIZE - self.slot_num * self.slot_table_entry_size(),
                             0, 0)
            self.slot_num -= 1
            return True
        # 获取被删除记录的长度
        _, remove_record_length = self.read_slot_entry(slot)
        # 选择覆盖的方式,
        move_slot_entry_start = OverFlowPage.cal_slot_entry_offset(self.slot_num - 1)
        move_slot_entry_end = OverFlowPage.cal_slot_entry_offset(slot)
        move_slot_len = move_slot_entry_end - move_slot_entry_start
        # 需要移动的记录的开始位值，也是新记录的写入位置，如果是追加数据，那么就是最新记录的结尾位置的下一个字节
        # 说明至少存在一个slot entry，需要进行移动
        if not move_slot_len == 0:
            # 移动数据长度
            move_record_len = 0
            # 读取尾部，获取移动数据的起始位置
            temp_slot_offset = move_slot_entry_end
            move_record_start = -1
            while temp_slot_offset > move_slot_entry_start:
                temp_slot_offset = temp_slot_offset - OverFlowPage.slot_table_entry_size()
                temp_record_offset, temp_record_length = struct.unpack_from('<ii', self.page_data, temp_slot_offset)
                if move_record_start == -1:
                    move_record_start = temp_record_offset
                move_record_len += temp_record_length
                # slot table的 偏移量也需要调整
                struct.pack_into('<i', self.page_data, temp_slot_offset, temp_record_offset - remove_record_length)
            # 进行数据的移动 record向后移动， slot table向前移动
            moved_record_offset = move_record_start - remove_record_length

            self.set_data(move_record_start, move_record_start + move_record_len,
                          moved_record_offset, moved_record_offset + move_record_len)
            # 调整slot table
            self.set_data(move_slot_entry_start, move_slot_entry_end,
                          move_slot_entry_start + self.slot_table_entry_size(),
                          move_slot_entry_end + self.slot_table_entry_size()
                          )

        self.slot_num -= 1
        return True

    def set_data(self, src_start: int, src_end: int, target_start: int, target_end: int):
        self.page_data[target_start:target_end] = self.page_data[src_start:src_end]

    def insert_shift(self, slot: int, record_length: int):
        """
        调整 原有的 record,slot entry 位置
        """
        # 新slot entry的放置位置
        new_slot_entry_start = OverFlowPage.cal_slot_entry_offset(slot)
        # 需要移动的slot
        move_slot_entry_start = OverFlowPage.cal_slot_entry_offset(self.slot_num - 1)
        move_slot_entry_end = OverFlowPage.cal_slot_entry_offset(slot - 1)
        move_slot_len = move_slot_entry_end - move_slot_entry_start
        # 需要移动的记录的开始位值，也是新记录的写入位置，如果是追加数据，那么就是最新记录的结尾位置的下一个字节
        move_record_start = self.position_for_append_record()
        # 说明至少存在一个slot entry，需要进行移动
        if not move_slot_len == 0:
            # 移动数据长度
            move_record_len = 0
            temp_slot_offset = move_slot_entry_end
            move_record_start = -1
            # 依次读取要移动的 slot entry，累加长度
            while temp_slot_offset > move_slot_entry_start:
                temp_slot_offset = temp_slot_offset - OverFlowPage.slot_table_entry_size()
                temp_offset, length = struct.unpack_from('<ii', self.page_data, temp_slot_offset)
                if move_record_start == -1:
                    move_record_start = temp_offset
                move_record_len += length
                # slot table的 偏移量也需要调整
                struct.pack_into('<i', self.page_data, temp_slot_offset, temp_offset + record_length)
            # 进行数据的移动 record向后移动， slot table向前移动
            # 要移动的数据的偏移 + 插入数据的长度， 就是移动后的偏移
            moved_record_offset = move_record_start + record_length
            self.set_data(move_record_start, move_record_start + move_record_len
                          , moved_record_offset, moved_record_offset + move_record_len)
            # 移动 slot table
            self.set_data(move_slot_entry_start, move_slot_entry_end
                          , move_slot_entry_start - self.slot_table_entry_size(),
                          move_slot_entry_end - self.slot_table_entry_size())
        return move_record_start, new_slot_entry_start

    def sync(self):
        struct.pack_into('<bii', self.page_data, 0, 1, self.slot_num, self.next_id)

    def get_next_record_id(self):
        """
        生成插入记录的record id
        """
        record_id = self.next_id
        self.next_id += 1
        return record_id

    def header_size(self) -> int:
        return 1 + 4 + 4

    def init_page(self):
        """
        写入头部信息
        """
        struct.pack_into('<bii', self.page_data, 0, 1, 0, 0)
