import struct

from typing import Tuple, List, Any

import config
from store.values import Row, ByteArray, Value, generate_row, over_flow_row

"""
slot table entry 大小固定
存放record的 offset 和 length
"""
SLOT_TABLE_ENTRY_SIZE = 4 + 4

"""
表示是over flow page
"""
OVER_FLOW_PAGE = 1
NORMAL_PAGE = 0
COMMON_PAGE = 2



def cal_slot_entry_offset(slot: int):
    """
    计算 slot entry的偏移量
    :param slot:
    :return:
    """
    return config.PAGE_SIZE - (slot + 1) * SLOT_TABLE_ENTRY_SIZE

def get_over_flow_value(row:Row)->bytearray:
    if not isinstance(row.values[0], ByteArray):
        raise TypeError('over_flow_page中只能插入bytearray')
    return row.values[0].get_bytes()

class BasePage:

    def __init__(self, page_num: int, page_data: bytearray):
        self.page_num = page_num
        self.page_data = page_data
        self.dirty = False
        self.slot_num = 0
        self.next_id = 0


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

    def delete_by_slot(self,slot:int)->int:
        """
        返回数据所处的record id
        :param slot:
        :return:
        """
        pass
    def delete_by_record_id(self, record_id:int)->int:
        """
        返回数据所处的slot
        :param record_id:
        :return:
        """
        pass

    def update_by_record_id(self,row:Row, record_id:int):
        pass

    def insert_to_last_slot(self,row:Row,record_id:int|None=None):
        """
        所有的新增只要追加在尾部就行， 修改 slot 的映射关系即可
        数据的逻辑顺序由slot决定， record实际在page上面的顺序是不固定的
        :param row:
        :param record_id:
        :return:
        """
        pass
    def insert_slot(self, row: Row, slot: int,record_id:int|None = None):
        pass
    def set_container(self, container):
        self.container = container

    def get_next_record_id(self):
        """
        生成插入记录的record id
        """
        record_id = self.next_id
        self.next_id += 1
        return record_id
    def init_page(self):
        """新创建页面时，需要初始化"""
        pass
    def read_slot_entry(self, slot) -> Tuple[int, int]:
        """
        返回 记录的: offset, length
        :param slot:
        :return:
        """
        offset = cal_slot_entry_offset(slot)
        return struct.unpack_from('<ii', self.page_data, offset)

    def cal_free_space(self):
        space_used = self.header_size() + self.slot_num * SLOT_TABLE_ENTRY_SIZE
        for i in range(self.slot_num):
            record_offset, record_len = self.read_slot_entry(i)
            space_used += record_len
        return config.PAGE_SIZE - space_used
    def header_records_length(self,free_space:int):
        """
        计算 page header + 所有record的长度
        :param free_space:
        :return:
        """
        return config.PAGE_SIZE - self.slot_num * SLOT_TABLE_ENTRY_SIZE - free_space

    def shrink(self, offset_start:int, shrink_bytes:int):
        """
        只shrink byte，不调整 slot table,以及 record中的长度信息
        :param offset_start: 移动byte的起始位置
        :param shrink_bytes: 移动字节数， 如果 >0 向后移动, 页面可用空间减少
        :return:
        """
        free_space = self.cal_free_space()
        if shrink_bytes > 0 and free_space < shrink_bytes:
            raise Exception('page溢出')
        if shrink_bytes < 0 and shrink_bytes < - offset_start- self.header_size():
            raise Exception('page溢出')
        content_len = self.header_records_length(free_space)
        self.page_data[offset_start + shrink_bytes:content_len + shrink_bytes] = \
            self.page_data[offset_start:content_len]

    def adjust_slot_record_length(self,slot:int,adjust:int):
        """
        调整 slot table中记录的record的长度
        :param slot:
        :param adjust:
        :return:
        """
        slot_offset = cal_slot_entry_offset(slot)
        length = struct.unpack_from('<i', self.page_data, slot_offset + 4)[0]
        struct.pack_into('<i', self.page_data, slot_offset + 4, length + adjust)


    def read_slot(self, slot: int) -> bytearray|None:
        """
        根据 slot位置读取数据
        :param slot:
        :return:
        """
        pass
    def read_record(self,record_id:int)->bytearray|None|Any:
        """
        根据record id 读取页的内容
        :param record_id:
        :return:
        """
        pass

    def position_for_append_record(self):
        """
         获取可以用于追加数据的位置
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
            struct.pack_into('<ii', self.page_data, config.PAGE_SIZE - self.slot_num * SLOT_TABLE_ENTRY_SIZE,
                             0, 0)
            self.slot_num -= 1
            return True
        # 获取被删除记录的长度
        _, remove_record_length = self.read_slot_entry(slot)
        # 选择覆盖的方式,
        move_slot_entry_start = cal_slot_entry_offset(self.slot_num - 1)
        move_slot_entry_end = cal_slot_entry_offset(slot)
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
                temp_slot_offset = temp_slot_offset - SLOT_TABLE_ENTRY_SIZE
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
                            move_slot_entry_start + SLOT_TABLE_ENTRY_SIZE,
                            move_slot_entry_end + SLOT_TABLE_ENTRY_SIZE
                            )

        self.slot_num -= 1
        return True
    def copy_slot(self,src_slot:int,target_slot:int)->None:
        """
         将slot table src_slot的内容拷贝到 target_slot
        :param src_slot:
        :param target_slot:
        :return:
        """
        #读取src_slot的内容
        record_offset,record_len = self.read_slot_entry(src_slot)
        struct.pack_into("<ii",self.page_data,cal_slot_entry_offset(target_slot),record_offset,record_len)
    def set_slot(self,slot:int,record_offset:int,record_len:int):
        struct.pack_into("<ii",self.page_data,cal_slot_entry_offset(slot),record_offset,record_len)

    def move_and_insert_slot(self,src_slot:int,target_slot:int):
        """
        移动一个slot插入到另一个slot的位置
        3 2 1 0
        移动3并插入到0的位置，那么就是：
        2 1 0 3
        :param src_slot:
        :param target_slot:
        :return:
        """
        if src_slot == target_slot:
            return
        src_record_offset,src_record_len = self.read_slot_entry(src_slot)
        if src_slot > target_slot:
             #向前移动
            for i in range(src_slot, target_slot,-1):
                self.copy_slot(i-1,i)
        else:
            #向后移动
            for i in range(src_slot, target_slot,1):
                self.copy_slot(i+1,i)
        self.set_slot(target_slot,src_record_offset,src_record_len)

    def insert_shift(self, slot: int, record_length: int):
        """
        调整 原有的 record,slot entry 位置
        """
        # 新slot entry的放置位置

        new_slot_entry_start = cal_slot_entry_offset(slot)
        # 需要移动的slot
        move_slot_entry_start = cal_slot_entry_offset(self.slot_num - 1)
        move_slot_entry_end = cal_slot_entry_offset(slot - 1)
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
                temp_slot_offset = temp_slot_offset - SLOT_TABLE_ENTRY_SIZE
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
                            , move_slot_entry_start - SLOT_TABLE_ENTRY_SIZE,
                            move_slot_entry_end - SLOT_TABLE_ENTRY_SIZE)
        return move_record_start, new_slot_entry_start



class OverFlowRecordHeader:
    def __init__(self, status:int, record_id:int, length:int, next_page_num:int, next_record_id:int):
        self.status = status
        self.record_id = record_id
        self.length = length
        self.next_page_num = next_page_num
        self.next_record_id = next_record_id

# record 单页存储完
SINGLE_PAGE = 1
# 多个页存储完
MULTI_PAGE = 0

class OverFlowPage(BasePage):
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
    def record_header_size() -> int:
        """记录头大小"""
        return 1 + 4 + 4 + 4 + 4

    @staticmethod
    def record_min_size() -> int:
        """
        存储一个记录最少需要的空间
        :return:
        """
        return OverFlowPage.record_header_size() + SLOT_TABLE_ENTRY_SIZE

    def read_record_header_by_slot(self,slot:int)->Tuple[int,OverFlowRecordHeader]:
        offset, _ = self.read_slot_entry(slot)
        return offset,OverFlowRecordHeader(*struct.unpack_from('<biiii', self.page_data, offset))

    def read_record_header_by_record_id(self,record_id:int)->Tuple[None|int,None|int,None|OverFlowRecordHeader]:
        for i in range(self.slot_num):
            offset,record_header = self.read_record_header_by_slot(i)
            if record_header.record_id == record_id:
                return offset,i,record_header
        return None,None,None

    def read_record(self, record_id:int)-> bytearray | None:
        result = bytearray()
        offset,slot,record_header = self.read_record_header_by_record_id(record_id)
        if not offset:
            return result
        data_offset = offset + OverFlowPage.record_header_size()
        result.extend(self.page_data[data_offset:data_offset + record_header.length])
        if record_header.status == MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            result.extend(next_page.read_record(record_header.next_record_id))
        return result

    def read_slot(self, slot: int) -> bytearray|None:
        if slot >= self.slot_num:
            return None
        result = bytearray()
        offset, length = self.read_slot_entry(slot)
        status,temp_record_id,length,next_page_num,next_record_id = struct.unpack_from('<biiii', self.page_data, offset)
        data_offset = offset + OverFlowPage.record_header_size()
        result.extend(self.page_data[data_offset:data_offset + length])
        if status == MULTI_PAGE:
            next_page = self.container.get_page(next_page_num)
            result.extend(next_page.read_record(next_record_id))
        return result

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

    def step_write(self,all_need_write_length:int,wrote_length:int):
        #本次写入需要的空间
        need_space = all_need_write_length - wrote_length + OverFlowPage.record_min_size()
        # data_length是record中单纯数据的长度
        status, data_length  = SINGLE_PAGE, 0
        free_space = self.cal_free_space()
        # 全部都可以放下
        if need_space <= free_space:
            data_length =  all_need_write_length - wrote_length
        else:
            status = MULTI_PAGE
            data_length = free_space - OverFlowPage.record_min_size()
        # 连头部信息，slot_table_entry_size 都不能存放
        if data_length < 0:
            return -1,-1
        return status,data_length
    def insert_to_last_slot(self,row:Row,record_id:int|None=None):
        return self.insert_slot(row,self.slot_num,record_id)

    def insert_slot(self, row: Row, slot: int,record_id:int|None = None):
        """
            返回插入记录的id 以及插入过程中写入的最大页码
        :return:
        """
        v = get_over_flow_value(row)
        cur_page = self

        if record_id:
            result_record_id=cur_record_id = record_id
        else:
            result_record_id=cur_record_id = self.get_next_record_id()
        #已经写入的数据长度
        wrote_length = 0
        #全部需要写入的数据长度
        all_need_write_length = len(v)
        while wrote_length < all_need_write_length:
            #获取本次写入的状态和长度
            status, data_length = cur_page.step_write(all_need_write_length,wrote_length)
            # 连头部信息，slot_table_entry_size 都不能存放
            if status == -1:
                return -1,-1
            #写入一条record的数据，返回record的偏移
            record_offset=cur_page.write_data(data_length, slot, status, cur_record_id ,v,wrote_length)
            cur_page.slot_num += 1
            wrote_length += data_length
            cur_page.sync()
            #需要写入到下一个页
            if status == MULTI_PAGE:
                next_page:OverFlowPage = cur_page.container.new_over_flow_page()
                #记录over flow信息
                #由于要写入下一个页，赋值cur_record_id为下一页的record_id
                cur_record_id =next_page.get_next_record_id()
                cur_page.adjust_record_over_flow(record_offset,next_page.page_num,cur_record_id)
                cur_page = next_page
                slot = cur_page.slot_num
        return result_record_id,cur_page.page_num

    def update_by_record_id(self, row: Row, record_id: int):
        """
         对于可变数据， 先删后加
        :param row:
        :param record_id:
        :return:
        """
        slot = self.delete_by_record_id(record_id)
        self.insert_slot(row,slot, record_id)

    def delete_by_slot(self,slot:int)->int:
        _,record_header= self.read_record_header_by_slot(slot)
        self.delete_shift(slot)
        ##删除下一页
        if record_header.status == MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            next_page.delete_by_record_id(record_header.next_record_id)
        self.sync()
        return record_header.record_id
    def delete_by_record_id(self, record_id:int)->int:
        """
        返回删除数据原先所处的slot
        :param record_id:
        :return:
        """
        _,slot,record_header=self.read_record_header_by_record_id(record_id)
        self.delete_shift(slot)
        ##删除下一页
        if record_header.status == MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            next_page.delete_by_record_id(record_header.next_record_id)
        self.sync()
        return slot


    def sync(self):
        struct.pack_into('<bii', self.page_data, 0, OVER_FLOW_PAGE, self.slot_num, self.next_id)
        self.dirty = True


    def header_size(self) -> int:
        return 1 + 4 + 4

    def init_page(self):
        """
        写入头部信息
        """
        struct.pack_into('<bii', self.page_data, 0, 1, 0, 0)


FIELD_NOT_OVER_FLOW = 0
FIELD_OVER_FLOW = 1
FIELD_OVER_FLOW_NULL = 2
FIELD_NOT_OVER_FLOW_NULL = 3

def get_value_status(v:Value):
    if v.len_variable():
        if v.is_null:
            return FIELD_OVER_FLOW_NULL
        else:
            return FIELD_OVER_FLOW
    if v.is_null:
        return FIELD_NOT_OVER_FLOW_NULL
    return FIELD_NOT_OVER_FLOW

# 可变长字段 占据的空间
OVER_FLOW_FIELD_LENGTH = 4 + 4

def cal_space_use(value:Value)->int:
    space_use = 0
    if value.len_variable():
        # status over flow page num , record id
        space_use+= 1 + OVER_FLOW_FIELD_LENGTH
    else:
        # 1 + field length field data
        space_use+=1 + 4 + value.space_use()
    return space_use

class StorePageRecorderHeader:
    def __init__(self, status:int, record_id:int, col_num:int, next_page_num:int, next_record_id:int):
        self.status = status
        self.record_id = record_id
        self.col_num = col_num
        self.next_page_num = next_page_num
        self.next_record_id = next_record_id
    def __repr__(self):
        return f'status={self.status},record_id={self.record_id},col_num={self.col_num},next_page_num={self.next_page_num},next_record_id={self.next_record_id}'
class Field:
   def __init__(self,status:int,offset:int):
       self.status = status
       #字段的偏移
       self.offset = offset
       self.value = None
       self.over_flow_page = None
       self.over_flow_record = None
   def is_null(self):
       return  self.status == FIELD_OVER_FLOW_NULL or self.status == FIELD_NOT_OVER_FLOW_NULL

   def is_over_flow(self):
       return self.status == FIELD_OVER_FLOW

   def set_value(self,value:bytearray):
       self.value = value
   def __repr__(self):
           return f'value:{self.value}'

class Record:
    def __init__(self,header:StorePageRecorderHeader,fields:List[Field]):
        self.header = header
        self.fields = fields
    def __repr__(self):
        return f'header={self.header},fields={self.fields}'

class StoredPage(BasePage):
    """
        结构:
     是否是 over_flow_page
     slot数量  4
     下一个可用的记录id  4
     当前页使用的over_flow_page的最大id
     record1,record2,record3 ....
     <slot table>
     record结构: record 不支持部分数据的 over flow 只进行全量的 over flow
      status 1
      record id 4
      page field数量 4
      over_flow_page number 4
      over_flow_page id 4
      field1,field2,field3 ....
    field结构:
        status: 1  字段状态
        如果不over，flow:
        fieldLength 4 fieldData
        如果over flow
        over_flow page_number  over_flow_record id   8
        如果null,长度依然保留，后续可能会更改内容

     <slot table>
      slot 偏移量  4
      slot 长度

    """

    def __init__(self, page_num: int, page_data: bytearray):
        super().__init__(page_num, page_data)
        self.page_type,self.slot_num,self.next_id,self.over_flow_page_num = struct.unpack_from('<biii', self.page_data, 0)
    def sync(self):
        struct.pack_into('<biii', self.page_data, 0, NORMAL_PAGE, self.slot_num, self.next_id, self.over_flow_page_num)
        self.dirty = True

    def header_size(self) -> int:
        return 1 + 4 + 4

    @staticmethod
    def normal_field_header_size():
        """
         status fieldLength
        :return:
        """
        return 1 + 4
    @staticmethod
    def record_header_size()->int:
        """
             record结构:
                  status 1
                  record id 4
                  page field数量 4
                  over_flow_page number 4
                  over_flow_page id 4
        """
        return 1 + 4 + 4 + 4 + 4
    @staticmethod
    def record_min_size()->int:
        """
        存储一个记录最少需要的空间
        :return:
        """
        return StoredPage.record_header_size() + SLOT_TABLE_ENTRY_SIZE
    @staticmethod
    def over_flow_field_min_size()->int:
        """
        status 1
        over_page_num
        :return:
        """


    def cal_write_page_field(self,row:Row):
        pass

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


    def write_data(self,col_num:int,data_length:int,slot:int,status:int,cur_record_id:int,source:bytearray,src_offset:int):
        """
        :param src_offset: 读取数据的偏移量
        :param source: 读取数据的元
        :param cur_record_id:
        :param status:
        :param slot 插入的位置
        :param cur_page:
        :param col_num 字段的数量
        :param data_length: 写入的数据部分的长度
        :return: 返回record的起始位置
        """
        # 包含了头部的record真正写入的长度
        record_length = data_length + StoredPage.record_header_size()
        record_start, slot_start = self.insert_shift(slot, record_length)
        # 写入头部
        struct.pack_into('<biiii', self.page_data, record_start, status, cur_record_id, col_num, -1,
                         -1)
        # 写入数据部分
        self.page_data[
        record_start + self.record_header_size():record_start + self.record_header_size() + data_length] \
            = source[src_offset:src_offset + data_length]
        # 写入 slot table
        struct.pack_into('<ii', self.page_data, slot_start, record_start, record_length)
        return record_start
    def get_over_flow_page(self,min_space:int|None = None):
        if self.over_flow_page_num == -1:
            over_page:OverFlowPage = self.container.new_over_flow_page()
            self.over_flow_page_num = over_page.page_num
        else:
            over_page:OverFlowPage = self.container.get_page(self.over_flow_page_num)
        if min_space and over_page.cal_free_space()<min_space:
            over_page:OverFlowPage = self.container.new_over_flow_page()
            self.over_flow_page_num = over_page.page_num
        return over_page

    def read_record_header_by_slot(self,slot:int)->Tuple[int,int,StorePageRecorderHeader]:
        offset, record_length = self.read_slot_entry(slot)
        return offset,record_length,StorePageRecorderHeader(*struct.unpack_from('<biiii', self.page_data, offset))

    def read_record_header_by_record_id(self,record_id:int)->Tuple[None|int,None|int,None|int,None|StorePageRecorderHeader]:
        """
        返回 记录偏移，记录长度 slot record_header

        """
        for i in range(self.slot_num):
            offset,record_length,record_header = self.read_record_header_by_slot(i)
            if record_header.record_id == record_id:
                return offset,record_length,i,record_header
        return None,None,None,None


    def get_row_data(self,row:Row)->bytearray:
        over_page = self.get_over_flow_page()
        row_data = bytearray()
        values:List[Value] = row.values
        for value in values:
            #写入定长字段
            status = get_value_status(value)
            if not value.len_variable():
                row_data.extend(struct.pack('<bi',status,value.space_use()))
                row_data.extend(value.get_bytes())
            else:
                #写入变长字段
                while True:
                    record_id,max_page_num = over_page.insert_to_last_slot(over_flow_row(value.get_bytes()))
                    if record_id == -1:
                        over_page = self.container.new_over_flow_page()
                        continue
                    row_data.extend(struct.pack('<bii',status,over_page.page_num,record_id))
                    self.over_flow_page_num = max_page_num
                    break
        return row_data

    def step_write(self,all_write_length:int,wrote_length:int):
        #本次写入需要的空间
        need_space = all_write_length - wrote_length + StoredPage.record_min_size()
        # data_length是record中单纯数据的长度
        status, data_length = SINGLE_PAGE, 0,
        free_space = self.cal_free_space()
        if need_space <= free_space:
            data_length =  all_write_length - wrote_length
        else:
            status = MULTI_PAGE
            data_length = free_space - StoredPage.record_min_size()
        # 连头部信息，slot_table_entry_size 都不能存放
        if data_length < 0:
            return -1,-1
        return status,data_length



    def insert_slot(self, row: Row, slot: int,record_id:int|None = None)->Tuple[int,int]:
        """
        返回插入的  record id 和 page num
        :param row:
        :param slot:
        :param record_id:
        :return:
        """
        if not record_id:
            record_id = self.get_next_record_id()

        #写入所有row的数据
        column_num = len(row.values)
        row_data = self.get_row_data(row)
        wrote_length = 0
        all_need_write_length = len(row_data)
        #获取本次写入的状态和长度
        status, data_length = self.step_write(all_need_write_length,wrote_length)
        #连头部信息到写入不进去
        if status == -1:
            return -1,-1
        #把在当页的写入
        record_offset = self.write_data(column_num,data_length, slot, status, record_id, row_data, wrote_length)
        self.slot_num += 1
        self.sync()
        if status == MULTI_PAGE:
            next_page:OverFlowPage = self.get_over_flow_page(StoredPage.record_min_size())
            #记录 over flow信息
            #由于要写入下一个页，赋值cur_record_id为下一页的record_id
            next_record_id =next_page.get_next_record_id()
            self.adjust_record_over_flow(record_offset,next_page.page_num,next_record_id)
            #剩下的所有内容写入 over flow page
            next_page.insert_to_last_slot(over_flow_row(row_data[data_length+StoredPage.record_min_size():]))
        return record_id,self.page_num

    def parse_record(self,row_data:bytearray, header:StorePageRecorderHeader)->Record:
        fields = []
        #在 row_data里面的偏移量
        offset = 0
        for i in range(header.col_num):
            status, = struct.unpack_from('<b',row_data,offset)
            field = Field(status,offset)
            offset +=1
            fields.append(field)
            if field.is_null():
                continue
            # over_flow_page num over_flow_record
            if  status == FIELD_OVER_FLOW:
                next_page_num,next_record_id = struct.unpack_from('<ii',row_data,offset)
                next_page:OverFlowPage = self.container.get_page(next_page_num)
                field.set_value(next_page.read_record(next_record_id))
                field.over_flow_page,field.over_flow_record = next_page_num,next_record_id
                offset+=4 + 4
            else:
                # fieldLength fieldData
                field_length, = struct.unpack_from('<i',row_data,offset)
                offset += 4
                field.set_value(row_data[offset:offset+field_length])
                offset+=field_length
        return Record(header,fields)

    def read_slot(self,slot:int)->Record|None:
        if slot >= self.slot_num:
            return None
        """
       读取 记录，以及记录所在的槽位
        """
        row_data = bytearray()
        offset,record_length,record_header = self.read_record_header_by_slot(slot)
        if not offset:
            return None
        data_offset = offset + StoredPage.record_header_size()
        row_data.extend(self.page_data[data_offset:data_offset + record_length])
        if record_header.status == MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            row_data.extend(next_page.read_record(record_header.next_record_id))
        record = self.parse_record(row_data,record_header)
        return record

    def read_record(self, record_id:int)->Tuple[Record|None,int|None]:
        """
        读取 记录，以及记录所在的槽位
        """
        row_data = bytearray()
        offset,record_length,slot,record_header = self.read_record_header_by_record_id(record_id)
        if not offset:
            return None,None
        data_offset = offset + StoredPage.record_header_size()
        # 记录长度 - 头部长度是数据部分长度
        data_length = record_length - self.record_header_size()
        row_data.extend(self.page_data[data_offset:data_offset + data_length])
        if record_header.status == MULTI_PAGE:
            next_page = self.container.get_page(record_header.next_page_num)
            row_data.extend(next_page.read_record(record_header.next_record_id))
        record = self.parse_record(row_data,record_header)
        return record,slot

    def set_field_null(self,field:Field):
        if field.status == FIELD_OVER_FLOW or field.status == FIELD_OVER_FLOW_NULL:
            struct.pack_into('<b',self.page_data,field.offset,FIELD_OVER_FLOW_NULL)
        else:
            struct.pack_into('<b',self.page_data,field.offset,FIELD_NOT_OVER_FLOW_NULL)
    def set_normal_field(self,field:Field,value:Value):
        # 跳过 status fieldLength 直接写入数据
        content = value.get_bytes()
        data_offset = field.offset + StoredPage.normal_field_header_size()
        self.page_data[data_offset:data_offset+len(content)] = content
    def remove_over_flow_field(self,field:Field):
        if field.is_null():
            return
        page:BasePage = self.container.get_page(field.over_flow_page)
        page.delete_by_record_id(field.over_flow_record)
    def set_over_flow_field(self,field:Field,over_flow_page_num:int,over_flow_record:int):
        struct.pack_into('<bii',self.page_data,field.offset,FIELD_OVER_FLOW,over_flow_page_num,over_flow_record)

    def update_by_record_id(self,row:Row, record_id:int):
        _,slot = self.read_record(record_id)
        self.update_by_slot(row,slot)

    def do_udpate(self,row:Row,record):
        pass
        # if not record.header.col_num == len(row.values):
        #     raise Exception('全量更新，需要指定所有列的内容')
        # fields = record.fields
        # over_page =self.get_over_flow_page()
        # for field,value in zip(fields,row.values):
        #     # 如果是普通数据类型
        #     if not field.is_over_flow():
        #         if value.is_null:
        #             self.set_field_null(field)
        #         else:
        #             self.set_normal_field(field, value)
        #     else:
        #         #删除已有内容
        #         self.remove_over_flow_field(field)
        #         if value.is_null:
        #             self.set_field_null(field)
        #         else:
        #             status = FIELD_OVER_FLOW
        #             #写入over flow
        #             while True:
        #                 record_id,max_page_num = over_page.insert_to_last_slot(over_flow_row(value.get_bytes()))
        #                 if record_id == -1:
        #                     over_page = self.container.new_over_flow_page()
        #                     continue
        #                 self.set_over_flow_field(field,over_page.page_num,record_id)
        #                 self.over_flow_page_num = max_page_num
        #                 break

    def update_by_slot(self,row:Row,slot:int):
        """
        暂时先全量更新 todo 后面优化，先跑起来
        :param row:
        :param slot:
        :return:
        """
        self.delete_by_slot(slot)
        self.insert_slot(row,slot)

    def delete_by_slot(self, slot: int):
        """
        返回删除数据原先所处的slot
        :param record_id:
        :return:
        """
        record = self.read_slot(slot)
        #先删除含有 over flow的 col
        for field in record.fields:
            if not field.value:
                continue
            if  field.over_flow_page:
                next_page = self.container.get_page(field.over_flow_page)
                next_page.delete_by_record_id(field.over_flow_record)
        #删除当页的数据
        self.delete_shift(slot)
        ##删除下一页
        if record.header.status == MULTI_PAGE:
            next_page = self.container.get_page(record.header.next_page_num)
            next_page.delete_by_record_id(record.header.next_record_id)
        self.sync()
        return slot

    def delete_by_record_id(self, record_id: int):
        """
        返回删除数据原先所处的slot
        :param record_id:
        :return:
        """
        record,slot = self.read_record(record_id)
        #先删除含有 over flow的 col
        for field in record.fields:
            if field.is_null():
                continue
            if field.is_over_flow():
                next_page = self.container.get_page(field.over_flow_page)
                next_page.delete_by_record_id(field.over_flow_record)
        #删除当页的数据
        self.delete_shift(slot)
        ##删除下一页
        if record.header.status == MULTI_PAGE:
            next_page = self.container.get_page(record.header.next_page_num)
            next_page.delete_by_record_id(record.header.next_record_id)
        self.sync()
        return slot

    def init_page(self):
        """
        写入头部信息
        """
        struct.pack_into('<biii', self.page_data, 0, NORMAL_PAGE, 0, 0,-1)
        self.over_flow_page_num = -1

class CommonPage(BasePage):
    """
        结构:
     是否是 over_flow_page
     slot数量  4
     下一个可用的记录id  4
     当前页使用的over_flow_page的最大id
     record1,record2,record3 ....
     <slot table>
     record结构: record
      status 1
      record id 4
      page field 在当页的field的数量（如果over flow， 如果没有overflow就是全部的数量）
      over_flow_page number 4
      over_flow_page id 4
      field1,field2,field3 ....
    field结构:
        status: 1  字段状态
        如果不over，flow:
        fieldLength 4 fieldData
        如果over flow
        fieldLength over_flow page_number  over_flow_record id   12
        如果null,长度依然保留，后续可能会更改内容
     <slot table>
      slot 偏移量  4
      slot 长度

    """
    def __init__(self, page_num: int, page_data: bytearray):
        super().__init__(page_num, page_data)
        self.page_type,self.slot_num,self.next_id,self.over_flow_page_num = struct.unpack_from('<biii', self.page_data, 0)
    def sync(self):
        struct.pack_into('<biii', self.page_data, 0, COMMON_PAGE, self.slot_num, self.next_id, self.over_flow_page_num)
        self.dirty = True

    def header_size(self) -> int:
        return 1 + 4 + 4

    @staticmethod
    def field_header_length():
        """
         status fieldLength
        :return:
        """
        return 1 + 4
    @staticmethod
    def record_header_size()->int:
        """
             record结构:
                  status 1
                  record id 4
                  page field数量 4
                  over_flow_page number 4
                  over_flow_page id 4
        """
        return 1 + 4 + 4 + 4 + 4
    @staticmethod
    def record_min_size()->int:
        """
        存储一个记录最少需要的空间
        :return:
        """
        return StoredPage.record_header_size() + SLOT_TABLE_ENTRY_SIZE
    @staticmethod
    def over_flow_field_data_size()->int:
        """
        不包含 头部信息
        over_page_num 4
        over_record_num 4
        :return:
        """
        return 4 + 4

    def get_over_flow_page(self,min_space:int|None = None):
        if self.over_flow_page_num == -1:
            over_page:OverFlowPage = self.container.new_common_page()
            self.over_flow_page_num = over_page.page_num
        else:
            over_page:OverFlowPage = self.container.get_page(self.over_flow_page_num)
        if min_space and over_page.cal_free_space()<min_space:
            over_page:OverFlowPage = self.container.new_common_page()
            self.over_flow_page_num = over_page.page_num
        return over_page

    def read_record_header_by_slot(self,slot:int)->Tuple[int,int,StorePageRecorderHeader]:
        offset, record_length = self.read_slot_entry(slot)
        return offset,record_length,StorePageRecorderHeader(*struct.unpack_from('<biiii', self.page_data, offset))

    def read_record_header_by_record_id(self,record_id:int)->Tuple[None|int,None|int,None|int,None|StorePageRecorderHeader]:
        """
        返回 记录偏移，记录长度 slot record_header

        """
        for i in range(self.slot_num):
            offset,record_length,record_header = self.read_record_header_by_slot(i)
            if record_header.record_id == record_id:
                return offset,record_length,i,record_header
        return None,None,None,None

    def insert_slot(self, row: Row, slot: int, record_id: int | None = None):
        #插入尾部
        page_num,record_id = self.insert_to_last_slot(row,record_id)
        if page_num == -1:
            return -1,-1
        #调整slot的位置
        #交换slot位置
        self.move_and_insert_slot(self.slot_num - 1,slot)

    def insert_to_last_slot(self, row: Row,record_id:int|None = None)->Tuple[int,int]:
        """
        返回插入的  record id 和 page num
        :param row:
        :param slot:
        :param record_id:
        :return:
        """
        if not record_id:
            record_id = self.get_next_record_id()
        #可用空间
        free_space = self.cal_free_space()
        # 不够写入header信息
        if free_space <  CommonPage.record_min_size():
            return -1,-1
        #获取写入数据的
        record_offset_start = self.header_records_length(free_space)

        slot_offset_start =  cal_slot_entry_offset(self.slot_num)
        #调整可用空间
        free_space-= CommonPage.record_min_size()
        #写入field的位置
        field_write_offset = record_offset_start + self.record_header_size()
        #逐个field的写入
        cols = 0
        while cols < len(row.values):
            value = row.values[cols]
            status = get_value_status(value)

            if value.is_null:
                if value.len_variable():
                    field_length =  CommonPage.over_flow_field_data_size()
                else:
                    field_length = value.space_use()
                if field_length + CommonPage.field_header_length() > free_space:
                    break
                #可以写入
                struct.pack_into('<bi',self.page_data,field_write_offset,status,field_length)
                field_write_offset+= CommonPage.field_header_length() + field_length
                free_space -= CommonPage.field_header_length() + field_length
            else:
                field_length = value.space_use()
                #当页全部放得下
                if field_length + CommonPage.field_header_length() <= free_space:
                    #写入数据部分
                    struct.pack_into('<bi',self.page_data,field_write_offset,FIELD_NOT_OVER_FLOW,field_length)
                    field_write_offset+= CommonPage.field_header_length()
                    self.page_data[field_write_offset:field_write_offset+field_length] = value.get_bytes()
                    field_write_offset+=  field_length
                    free_space -= CommonPage.field_header_length() + field_length
                #当页不能写完,只能写入一部分,剩下的需要over flow
                else:
                    #连over flow空间都没有，直接结束
                    if free_space< CommonPage.field_header_length()+ CommonPage.over_flow_field_data_size():
                        break
                    #当页可以写入的长度
                    cur_page_len = free_space - CommonPage.field_header_length() -  CommonPage.over_flow_field_data_size()
                    #剩余部分写入over flow page
                    new_row = generate_row([value.get_bytes()[cur_page_len:]])
                    over_page=self.get_over_flow_page(CommonPage.record_min_size())
                    over_page_num,over_page_record_id = over_page.insert_to_last_slot(new_row)
                    struct.pack_into('<biii',self.page_data,field_write_offset,FIELD_OVER_FLOW,cur_page_len,over_page_num,over_page_record_id)
                    field_write_offset+= CommonPage.field_header_length()
                    self.page_data[field_write_offset:field_write_offset+cur_page_len] = value.get_bytes()[0:cur_page_len]
                    field_write_offset+= field_length
                    free_space -= CommonPage.field_header_length() + field_length
            cols+=1

        if cols < len(row.values):
            status = MULTI_PAGE
            over_page = self.get_over_flow_page(CommonPage.record_min_size())
            over_page_num,over_page_record_id = over_page.insert_to_last_slot(row.sub_row(cols))
            struct.pack_into('<biiii', self.page_data, record_offset_start, status, record_id, cols, over_page_num,
                             over_page_record_id)
        else:
            status = SINGLE_PAGE
            struct.pack_into('<biiii', self.page_data, record_offset_start, status, record_id, cols, -1,
                         -1)
        #写入尾部信息
        # 写入 slot table
        struct.pack_into('<ii', self.page_data, slot_offset_start, record_offset_start,field_write_offset - record_offset_start)
        self.slot_num+=1
        #把在当页的写入
        self.sync()
        return record_id,self.page_num

    def set_field_null(self,field:Field):
        if field.status == FIELD_OVER_FLOW or field.status == FIELD_OVER_FLOW_NULL:
            struct.pack_into('<b',self.page_data,field.offset,FIELD_OVER_FLOW_NULL)
        else:
            struct.pack_into('<b',self.page_data,field.offset,FIELD_NOT_OVER_FLOW_NULL)
    def set_normal_field(self,field:Field,value:Value):
        # 跳过 status fieldLength 直接写入数据
        content = value.get_bytes()
        data_offset = field.offset + CommonPage.field_header_length()
        self.page_data[data_offset:data_offset+len(content)] = content
    def remove_over_flow_field(self,field:Field):
        if field.is_null():
            return
        page:BasePage = self.container.get_page(field.over_flow_page)
        page.delete_by_record_id(field.over_flow_record)
    def set_over_flow_field(self,field:Field,over_flow_page_num:int,over_flow_record:int):
        struct.pack_into('<bii',self.page_data,field.offset,FIELD_OVER_FLOW,over_flow_page_num,over_flow_record)

    def init_page(self):
        """
        写入头部信息
        """
        struct.pack_into('<biii', self.page_data, 0, COMMON_PAGE, 0, 0,-1)
        self.over_flow_page_num = -1
