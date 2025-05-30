
import config
import file_util
import os
import struct
from typing import Dict, Tuple

from store.values import  Row


class BasePage:
    OVER_FLOW_PAGE = 1

    def __init__(self, page_num: int, page_data: bytearray):
        self.page_num = page_num
        self.page_data = page_data
        self.dirty = False

    def sync(self):
        """
        状态同步到 page_data中
        :return:
        """
        pass

    def header_size(self) -> int:
        pass
    def shift_data(self, src_start: int, src_end: int, target_start: int, target_end: int):
        """
        移动page data内部的数据
        :param src_start:
        :param src_end:
        :param target_start:
        :param target_end:
        :return:
        """
        self.page_data[target_start:target_end] = self.page_data[src_start:src_end]

    def insert_slot(self, row: Row, slot: int):
        pass
    def set_container(self, container):
        self.container = container

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
        return page_num

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
    def __init__(self, path: str | None = None):
        self.path = path
        file_util.create_file_if_need(self.path)
        self.alloc = ContainerAlloc(self.path)
        self.file = open(self.path, 'rb+')
        self.cache:Dict[int,BasePage]={}

    def get_size(self):
        return os.path.getsize(self.path)

    def seek_page(self, page_number: int):
        offset = page_number * config.PAGE_SIZE
        self.file.seek(offset)

    def seek_offset(self, offset: int):
        self.file.seek(offset)

    def read_page(self, page_number: int, page_data: bytearray):
        self.seek_page(page_number)
        page_data.extend(self.file.read(config.PAGE_SIZE))

    def pad_file(self, offset: int):
        cur_eof = self.get_size()
        self.file.seek(cur_eof)
        zero_data = bytearray(config.PAGE_SIZE)
        while cur_eof < offset:
            diff = offset - cur_eof
            if diff > config.PAGE_SIZE:
                diff = config.PAGE_SIZE
                self.file.write(zero_data[:diff])
            cur_eof = cur_eof + diff

    def write_page(self, page_number: int, page_data: bytearray):
        offset = page_number * config.PAGE_SIZE
        self.seek_offset(offset)
        if  self.get_size() < offset:
            self.pad_file(offset)
        self.file.write(page_data)

    def add_page(self):
        pass

    def init_page(self):
        pass

    def get_page(self,page_num:int):
        if page_num in self.cache:
            return self.cache[page_num]
        page_data = bytearray()
        self.read_page(page_num, page_data)
        page = OverFlowPage(page_num, page_data)
        page.set_container(self)
        self.cache[page_num] = page
        return page

    def new_page(self) -> BasePage:
        """
        新创建需要初始化
        :return:
        """
        page_data = bytearray(config.PAGE_SIZE)
        page_num = self.alloc.alloc()
        page = OverFlowPage(page_num, page_data)
        page.set_container(self)
        page.init_page()
        self.cache[page_num] = page
        return page

    def close(self):
        self.alloc.close()
        self.file.close()

    def flush(self):
        for k, v in self.cache.items():
            if v.dirty:
                self.write_page(k,v.page_data)
        self.alloc.flush()
        self.file.flush()

class OverFlowRecordHeader:
    def __init__(self, status:int, record_id:int, length:int, next_page_num:int, next_record_id:int):
        self.status = status
        self.record_id = record_id
        self.length = length
        self.next_page_num = next_page_num
        self.next_record_id = next_record_id


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


    @staticmethod
    def cal_slot_entry_offset(slot: int):
        return config.PAGE_SIZE - (slot + 1) * OverFlowPage.slot_table_entry_size()

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
        return config.PAGE_SIZE - space_used

    def read_record_header_by_slot(self,slot:int)->Tuple[int,OverFlowRecordHeader]:
        offset, _ = self.read_slot_entry(slot)
        return offset,OverFlowRecordHeader(*struct.unpack_from('<biiii', self.page_data, offset))

    def read_record_header_by_record_id(self,record_id:int)->Tuple[None,None,None]|Tuple[int,int,OverFlowRecordHeader]:
        for i in range(self.slot_num):
            offset,record_header = self.read_record_header_by_slot(i)
            if record_header.record_id == record_id:
                return offset,i,record_header
        return None,None,None

    def read_by_record_id(self, record_id:int)-> bytearray | None:
        result = bytearray()
        offset,slot,record_header = self.read_record_header_by_record_id(record_id)
        if not offset:
            return result
        data_offset = offset + OverFlowPage.record_header_size()
        result.extend(self.page_data[data_offset:data_offset + record_header.length])
        if record_header.status == OverFlowPage.MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            result.extend(next_page.read_by_record_id(record_header.next_record_id))
        return result


    def read_slot(self, slot: int) -> bytearray|None:
        if slot >= self.slot_num:
            return None
        result = bytearray()
        offset, length = self.read_slot_entry(slot)
        status,temp_record_id,length,next_page_num,next_record_id = struct.unpack_from('<biiii', self.page_data, offset)
        data_offset = offset + OverFlowPage.record_header_size()
        result.extend(self.page_data[data_offset:data_offset + length])
        if status == OverFlowPage.MULTI_PAGE:
            next_page = self.container.get_page(next_page_num)
            result.extend(next_page.read_by_record_id(next_record_id))
        return result


    @staticmethod
    def get_over_flow_value(row:Row)->bytearray:
        if not isinstance(row.values[0], bytearray):
            raise TypeError('over_flow_page中只能插入bytearray')
        return row.values[0]


    def adjust_record_over_flow(self,record_offset,next_page_num:int,next_page_record_id:int):
        """
            调整记录的 over flow 信息
        :param record_offset:
        :param next_page_num:
        :param next_page_record_id:
        :return:
        """
        #跳过record的头部信息，直接调整 next_page_num next_page_record_id
        struct.pack_into('ii',self.page_data,record_offset + 1 + 4 + 4,next_page_num,next_page_record_id)

    def write_data(self,data_length:int,slot:int,status:int,cur_record_id:int,source:bytearray,src_offset:int):
        """
        :param src_offset: 读取数据的偏移量
        :param source: 读取数据的元
        :param cur_record_id:
        :param status:
        :param slot 插入的位置
        :param cur_page:
        :param data_length: 写入的数据部分的长度
        :return: 返回record的起始位置
        """
        # 包含了头部的record真正写入的长度
        record_length = data_length + OverFlowPage.record_header_size()
        record_start, slot_start = self.insert_shift(slot, record_length)
        # 写入头部
        struct.pack_into('<biiii', self.page_data, record_start, status, cur_record_id, data_length, -1,
                         -1)
        # 写入数据部分
        self.page_data[
        record_start + self.record_header_size():record_start + self.record_header_size() + data_length] \
            = source[src_offset:src_offset + data_length]
        # 写入 slot table
        struct.pack_into('<ii', self.page_data, slot_start, record_start, record_length)
        return record_start

    def insert_slot(self, row: Row, slot: int):
        """
            返回插入记录的id 以及插入过程中写入的最大页码
        :return:
        """
        v = OverFlowPage.get_over_flow_value(row)
        cur_page = self
        result_record_id=cur_record_id = self.get_next_record_id()
        #已经写入的数据长度
        wrote_length = 0
        #全部需要写入的数据长度
        all_need_write_length = len(v)
        while wrote_length < all_need_write_length:
            #本次写入需要的空间
            need_space = all_need_write_length - wrote_length + OverFlowPage.record_min_size()
            # data_length是record中单纯数据的长度
            status, data_length, next_page_num, next_record_id = OverFlowPage.SINGLE_PAGE, 0, -1, -1
            free_space = cur_page.cal_free_space()
            # 全部都可以放下
            if need_space <= free_space:
                data_length =  all_need_write_length - wrote_length
            else:
                status = OverFlowPage.MULTI_PAGE
                data_length = free_space - OverFlowPage.record_min_size()
            # 连头部信息，slot_table_entry_size 都不能存放
            if data_length < 0:
                return -1
            #写入一条record的数据，返回record的便宜
            record_offset=cur_page.write_data(data_length, slot, status, cur_record_id ,v,wrote_length)
            cur_page.slot_num += 1
            wrote_length += data_length
            cur_page.sync()
            #需要写入到下一个页
            if status == OverFlowPage.MULTI_PAGE:
                next_page:OverFlowPage = cur_page.container.new_page()
                #记录over flow信息
                #由于要写入下一个页，赋值cur_record_id为下一页的record_id
                cur_record_id =next_page.get_next_record_id()
                cur_page.adjust_record_over_flow(record_offset,next_page.page_num,cur_record_id)
                cur_page = next_page
                slot = 0
        return result_record_id,cur_page.page_num

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

    def delete_by_slot(self,slot:int):
        _,record_header= self.read_record_header_by_slot(slot)
        self.delete_shift(slot)
        ##删除下一页
        if record_header.status == OverFlowPage.MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            next_page.delete_by_record_id(record_header.next_record_id)
        self.sync()
    def delete_by_record_id(self, record_id:int):
        _,slot,record_header=self.read_record_header_by_record_id(record_id)
        self.delete_shift(slot)
        ##删除下一页
        if record_header.status == OverFlowPage.MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            next_page.delete_by_record_id(record_header.next_record_id)
        self.sync()


    def delete_shift(self, slot: int) -> bool:
        if slot >= self.slot_num:
            return False
        # 删除最后一个，使用填充0的方式
        if slot == self.slot_num - 1:
            struct.pack_into('<ii', self.page_data, config.PAGE_SIZE - self.slot_num * self.slot_table_entry_size(),
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

            self.shift_data(move_record_start, move_record_start + move_record_len,
                            moved_record_offset, moved_record_offset + move_record_len)
            # 调整slot table
            self.shift_data(move_slot_entry_start, move_slot_entry_end,
                            move_slot_entry_start + self.slot_table_entry_size(),
                            move_slot_entry_end + self.slot_table_entry_size()
                            )

        self.slot_num -= 1
        return True


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
            self.shift_data(move_record_start, move_record_start + move_record_len
                            , moved_record_offset, moved_record_offset + move_record_len)
            # 移动 slot table
            self.shift_data(move_slot_entry_start, move_slot_entry_end
                            , move_slot_entry_start - self.slot_table_entry_size(),
                            move_slot_entry_end - self.slot_table_entry_size())
        return move_record_start, new_slot_entry_start

    def sync(self):
        struct.pack_into('<bii', self.page_data, 0, 1, self.slot_num, self.next_id)
        self.dirty = True

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
