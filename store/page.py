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

    def is_over_flow(self)->bool:
        pass


    def increase_slot_num(self):
        self.slot_num+=1
        self.sync()
    def decrease_slot_num(self):
        self.slot_num-=1
        self.sync()

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
    def init_page(self,is_over_flow:bool=  False):
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
        shrink byte，并调整 slot table,以及 record中的长度信息
        :param offset_start: 移动byte的起始位置
        :param shrink_bytes: 移动字节数， 如果 >0 向后移动, 页面可用空间减少
        :return:
        """
        #获取offset_start之后的所有slot
        shrink_slot = []
        for i in range(self.slot_num):
            record_offset,record_len = self.read_slot_entry(i)
            if record_offset >= offset_start:
                #记录slot num 并调整record偏移
                shrink_slot.append((i,record_offset+shrink_bytes))
        free_space = self.cal_free_space()
        if shrink_bytes > 0 and free_space < shrink_bytes:
            raise Exception('page溢出')
        if shrink_bytes < 0 and shrink_bytes < - offset_start- self.header_size():
            raise Exception('page溢出')
        content_len = self.header_records_length(free_space)
        self.page_data[offset_start + shrink_bytes:content_len + shrink_bytes] = \
            self.page_data[offset_start:content_len]

        #调整slot的偏移量
        for slot,record_offset in shrink_slot:
            self.set_slot_record_offset(slot,record_offset)

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


    def read_slot(self, slot: int) -> Any:
        """
        根据 slot位置读取数据
        :param slot:
        :return:
        """
        pass
    def read_record(self,record_id:int)->Any:
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
    def set_slot_record_offset(self,slot:int,record_offset:int):
        struct.pack_into("<i",self.page_data,cal_slot_entry_offset(slot),record_offset)

    def search_slot_by_record_offset(self,offset:int):
        for i in range(self.slot_num):
            record_offset,record_len =self.read_slot_entry(i)
            if record_offset == offset:
                return i
        return -1

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


# 字段的状态
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

# 字段读取的设置
#1.读取数据
INCLUDE_FIELD_DATA = 0
#2.只读取字段的头部
ONLY_FIELD_HEADER = 1



def cal_space_use(value:Value)->int:
    space_use = 0
    if value.len_variable():
        # status over flow page num , record id
        space_use+= 1 + OVER_FLOW_FIELD_LENGTH
    else:
        # 1 + field length field data
        space_use+=1 + 4 + value.space_use()
    return space_use

class CommonPageRecordHeader:
    def __init__(self, status:int, record_id:int, col_num:int, next_page_num:int, next_record_id:int):
        self.status = status
        self.record_id = record_id
        self.col_num = col_num
        self.next_page_num = next_page_num
        self.next_record_id = next_record_id
    def __repr__(self):
        return f'status={self.status},record_id={self.record_id},col_num={self.col_num},next_page_num={self.next_page_num},next_record_id={self.next_record_id}'
class Field:
   def __init__(self,status:int,page_num:int,offset:int):
       self.status = status
       #字段所处的page
       self.page_num = page_num
       #字段的偏移
       self.offset = offset
       self.value = bytearray()
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
    def __init__(self, header:CommonPageRecordHeader, fields:List[Field]):
        self.header = header
        self.fields = fields
    def __repr__(self):
        return f'header={self.header},fields={self.fields}'

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
        struct.pack_into('<biii', self.page_data, 0, self.page_type, self.slot_num, self.next_id, self.over_flow_page_num)
        self.dirty = True

    def header_size(self) -> int:
        return 1 + 4 + 4 + 4

    @staticmethod
    def over_flow_field_header():
        return CommonPage.field_header_length() + CommonPage.over_flow_field_data_size()

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
        return CommonPage.record_header_size() + SLOT_TABLE_ENTRY_SIZE
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
            over_page = self.container.new_common_page(is_over_flow=True)
            self.over_flow_page_num = over_page.page_num
        else:
            over_page = self.container.get_page(self.over_flow_page_num)
        if min_space and over_page.cal_free_space()<min_space:
            over_page = self.container.new_common_page(is_over_flow=True)
            self.over_flow_page_num = over_page.page_num
        return over_page

    def get_slot_num_by_record_id(self,record_id:int):
        for i in range(self.slot_num):
            offset, record_length = self.read_slot_entry(i)
            _,temp_record_id = struct.unpack_from('<bi', self.page_data, offset)
            if temp_record_id == record_id:
                return i
        return -1

    def read_record_header_by_slot(self,slot:int)->Tuple[int,int,CommonPageRecordHeader]:
        record_offset, record_length = self.read_slot_entry(slot)
        return record_offset,record_length,CommonPageRecordHeader(*struct.unpack_from('<biiii', self.page_data, record_offset))

    def read_record_header_by_record_id(self,record_id:int)->Tuple[int, int, int,CommonPageRecordHeader]:
        """
        返回 记录偏移，记录长度 slot record_header
        """
        slot = self.get_slot_num_by_record_id(record_id)
        record_offset, record_length = self.read_slot_entry(slot)
        record_header = CommonPageRecordHeader(*struct.unpack_from('<biiii', self.page_data, record_offset))
        return record_offset,record_length,slot,record_header

    def insert_slot(self, row: Row, slot: int, record_id: int | None = None):
        #插入尾部
        page_num,record_id = self.insert_to_last_slot(row,record_id)
        if page_num == -1:
            return -1,-1
        #调整slot的位置
        #交换slot位置
        self.move_and_insert_slot(self.slot_num - 1,slot)

    def write_long_field(self,field:Value,write_from,over_record_id:int,page:BasePage):
        cur_record_id = over_record_id
        cur_page = page
        field_bytes = field.get_bytes()
        field_size= len(field_bytes)
        #每次都尝试把页写满
        while write_from < field_size:
            free_space = cur_page.cal_free_space()
            #还需要写的数据长度
            field_length = field_size - write_from
            #获取写入数据的
            record_offset_start = cur_page.header_records_length(free_space)
            slot_offset_start =  cal_slot_entry_offset(cur_page.slot_num)
            #调整可用空间
            free_space -= CommonPage.record_min_size()
            #写入field的位置
            field_write_offset = record_offset_start + cur_page.record_header_size()
            #写入field
            # 1. 本次可以完全写完
            if field_length + CommonPage.field_header_length() <= free_space:
                #写入数据部分
                struct.pack_into('<bi',cur_page.page_data,field_write_offset,FIELD_NOT_OVER_FLOW,field_length)
                cur_page.page_data[field_write_offset + CommonPage.field_header_length():field_write_offset+CommonPage.field_header_length()+field_length] = field.get_bytes()[write_from:]
                #写入 record header
                struct.pack_into('<biiii', cur_page.page_data, record_offset_start, SINGLE_PAGE, cur_record_id, 1, -1,
                                 -1)
                #写入 slot table
                struct.pack_into('<ii', cur_page.page_data, slot_offset_start, record_offset_start,field_write_offset+CommonPage.field_header_length()+field_length - record_offset_start)
                write_from+=field_length
                cur_page.increase_slot_num()
                break
            #写多次
            else:
                cur_page_len= free_space - CommonPage.over_flow_field_header()
                over_page=cur_page.get_over_flow_page(CommonPage.record_min_size() + CommonPage.field_header_length()+ min(cur_page_len,10))
                over_page_record_id = over_page.get_next_record_id()
                struct.pack_into('<biii',cur_page.page_data,field_write_offset,FIELD_OVER_FLOW,cur_page_len,over_page.page_num,over_page_record_id)
                cur_page.page_data[field_write_offset+CommonPage.over_flow_field_header():CommonPage.over_flow_field_header()+field_write_offset+cur_page_len] = field.get_bytes()[write_from:write_from+cur_page_len]
                write_from+=cur_page_len
                #写入 record header
                struct.pack_into('<biiii', cur_page.page_data, record_offset_start, MULTI_PAGE, cur_record_id, 1, over_page.page_num,over_page_record_id)
                #写入 slot table
                struct.pack_into('<ii', cur_page.page_data, slot_offset_start, record_offset_start,field_write_offset+CommonPage.over_flow_field_header()+cur_page_len - record_offset_start)
                cur_page.increase_slot_num()
                #多的写入下一页
                cur_page = over_page
                cur_record_id = over_record_id


    def write_field(self,field:Value,page,free_space:int,write_offset:int):
        """
        """
        status = get_value_status(field)
        if field.is_null:
            #预留一个 over flow的空间
            if field.len_variable():
                field_length =  CommonPage.over_flow_field_data_size()
            else:
                field_length = field.space_use()
            if field_length + CommonPage.field_header_length() > free_space:
                return -1
            #可以写入
            struct.pack_into('<bi',page.page_data,write_offset,status,field_length)
            return CommonPage.field_header_length() + field_length
        else:
            field_length = field.space_use()
            #当页全部放得下
            if field_length + CommonPage.field_header_length() <= free_space:
                #写入数据部分
                struct.pack_into('<bi',page.page_data,write_offset,FIELD_NOT_OVER_FLOW,field_length)
                page.page_data[write_offset + CommonPage.field_header_length():write_offset+CommonPage.field_header_length()+field_length] = field.get_bytes()
                return CommonPage.field_header_length() + field_length
            #当页不能写完,只能写入一部分,剩下的需要over flow
            else:
                #连over flow空间都没有，直接结束
                if free_space< CommonPage.over_flow_field_header():
                    return -1
                #当页可以写入的长度
                cur_page_len = free_space - CommonPage.over_flow_field_header()
                #新开辟的页，最少也要有写入一个field的空间
                over_page=page.get_over_flow_page(CommonPage.record_min_size() + CommonPage.field_header_length()+ min(cur_page_len,10))
                over_page_record_id = over_page.get_next_record_id()
                struct.pack_into('<biii',self.page_data,write_offset,FIELD_OVER_FLOW,cur_page_len,over_page.page_num,over_page_record_id)
                self.page_data[write_offset+CommonPage.over_flow_field_header():CommonPage.over_flow_field_header()+write_offset+cur_page_len] = field.get_bytes()[0:cur_page_len]
                #剩余的写入over_page
                self.write_long_field(field,cur_page_len,over_page_record_id,over_page)
                # fielder header + over flow信息 + 当页写入信息
                return cur_page_len + CommonPage.over_flow_field_header()


    def insert_to_last_slot(self, row: Row,record_id:int|None = None)->Tuple[int,int]:
        """
        返回插入的  record id 和 page num
        :param row:
        :param slot:
        :param record_id:
        :return:
        """
        if not record_id:
            record_id = cur_record_id = self.get_next_record_id()
        else:
            cur_record_id = record_id
        cur_page = self
        #逐个field的写入
        wrote_cols = 0
        while wrote_cols < len(row.values):
            #可用空间
            free_space = cur_page.cal_free_space()
            # 不够写入header信息
            if free_space <  CommonPage.record_min_size():
                return -1,-1
            #获取写入数据的
            record_offset_start = cur_page.header_records_length(free_space)

            slot_offset_start =  cal_slot_entry_offset(cur_page.slot_num)
            #调整可用空间
            free_space-= CommonPage.record_min_size()
            #写入field的位置
            field_write_offset = record_offset_start + cur_page.record_header_size()
            step_wrote_cols = 0
            while wrote_cols + step_wrote_cols < len(row.values):
                value = row.values[wrote_cols+step_wrote_cols]
                write_len = cur_page.write_field(value,cur_page,free_space,field_write_offset)
                if write_len == -1:
                    break
                free_space -= write_len
                field_write_offset+=write_len
                step_wrote_cols+=1
            print(f'page:{cur_page.page_num},cols:{step_wrote_cols}')
            #当页写完
            if wrote_cols +step_wrote_cols < len(row.values):
                over_page = cur_page.get_over_flow_page(CommonPage.record_min_size())
                over_record_id = over_page.get_next_record_id()
                struct.pack_into('<biiii', cur_page.page_data, record_offset_start, MULTI_PAGE, cur_record_id, step_wrote_cols, over_page.page_num,
                                 over_record_id)
                struct.pack_into('<ii', cur_page.page_data, slot_offset_start, record_offset_start,field_write_offset - record_offset_start)
                cur_page.increase_slot_num()
                cur_record_id = over_record_id
                cur_page = over_page
            else:
                cur_page.increase_slot_num()
                struct.pack_into('<biiii', cur_page.page_data, record_offset_start, SINGLE_PAGE, cur_record_id, step_wrote_cols, -1,
                                 -1)
                struct.pack_into('<ii', cur_page.page_data, slot_offset_start, record_offset_start,field_write_offset - record_offset_start)
            wrote_cols+=step_wrote_cols

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

    def is_over_flow(self) -> bool:
        return self.page_type == OVER_FLOW_PAGE

    def init_page(self,is_over_flow:bool=False):
        """
        写入头部信息
        """
        if is_over_flow:
            struct.pack_into('<biii', self.page_data, 0, OVER_FLOW_PAGE, 0, 0,-1)
        else:
            struct.pack_into('<biii', self.page_data, 0, NORMAL_PAGE, 0, 0,-1)
        self.over_flow_page_num = -1


    def delete_by_record_id(self, record_id: int):
        self._delete_record_id(record_id)

    def read_record(self,record_id:int) ->Record:
        slot = self.get_slot_num_by_record_id(record_id)
        return self.read_slot(slot)

    def read_over_flow_field(self,next_page_num:int,next_record_id:int,field:Field):
        """
        over field 占用的record只会有一个col
        :param next_page_num:
        :param next_record_id:
        :param field:
        :return:
        """
        cur_page = self.container.get_page(next_page_num)
        cur_slot = self.get_slot_num_by_record_id(next_record_id)
        while True:
            record_offset,_,record_header =  cur_page.read_record_header_by_slot(cur_slot)
            field_offset = record_offset+ cur_page.record_header_size()
            status,field_length = struct.unpack_from('<bi',cur_page.page_data,field_offset)
            field_offset += cur_page.field_header_length()
            if status == FIELD_NOT_OVER_FLOW:
                #读取结束
                field.value.extend(cur_page.page_data[field_offset:field_offset+field_length])
                return
            else:
                temp_next_page_num,temp_next_record_id = struct.unpack_from('<ii',cur_page.page_data,field_offset)
                #跳过over flow部分
                field_offset += self.over_flow_field_data_size()
                #读取数据
                field.value.extend(cur_page.page_data[field_offset:field_offset+field_length])
                cur_page = self.container.get_page(temp_next_page_num)
                cur_slot = cur_page.get_slot_num_by_record_id(temp_next_record_id)

    def update_field_by_index(self,record_id:int,field_index:int,value:Value):
        pass
    def update_field(self,field:Field,value:Value):
        pass
    def read_slot(self, slot: int) ->Record:
        cur_slot = slot
        cur_page = self
        record_offset,_,record_header =  cur_page.read_record_header_by_slot(cur_slot)
        result_record_header = record_header
        fields_list = []
        while True :
            #读取field
            field_offset = record_offset+ cur_page.record_header_size()
            for i in range(record_header.col_num):
                status,field_length = struct.unpack_from('<bi',cur_page.page_data,field_offset)
                field = Field(status,cur_page.page_num,field_offset)
                fields_list.append(field)
                field_offset += cur_page.field_header_length()
                if status == FIELD_OVER_FLOW_NULL or status == FIELD_NOT_OVER_FLOW_NULL:
                    pass
                elif  status == FIELD_NOT_OVER_FLOW:
                    #读取数据
                    field.value.extend(cur_page.page_data[field_offset:field_offset+field_length])
                    field_offset += field_length
                else:
                    next_page_num,next_record_id = struct.unpack_from('<ii',cur_page.page_data,field_offset)
                    #跳过over flow部分
                    field_offset += self.over_flow_field_data_size()
                    #读取数据
                    field.value.extend(cur_page.page_data[field_offset:field_offset+field_length])
                    field_offset += field_length
                    cur_page.read_over_flow_field(next_page_num, next_record_id, field)
            #读取下一页
            if not record_header.next_page_num == -1:
                cur_page=  self.container.get_page(record_header.next_page_num)
                cur_slot = cur_page.get_slot_num_by_record_id(record_header.next_record_id)
                record_offset,_,record_header =  cur_page.read_record_header_by_slot(cur_slot)
            else:
                break
        return Record(result_record_header,fields_list)

    def _delete_record_id(self,record_id:int):
        """
        :return:
        """
        deleted = []
        wait_deleted = [(self.page_num,record_id)]
        while len(wait_deleted) >0:
            cur_page_num,cur_record_id = wait_deleted.pop(0)
            if (cur_page_num,record_id) in deleted:
                continue
            print(f'delete: {cur_page_num} {cur_record_id}')
            deleted.append((cur_page_num,cur_record_id))
            cur_page = self.container.get_page(cur_page_num)
            record_offset,record_length,slot,record_header =  cur_page.read_record_header_by_record_id(cur_record_id)
            if record_offset is None:
                return
            #读取field
            field_offset = record_offset+ cur_page.record_header_size()
            for i in range(record_header.col_num):
                status,field_length = struct.unpack_from('<bi',cur_page.page_data,field_offset)
                field_offset += cur_page.field_header_length()
                if  status == FIELD_OVER_FLOW:
                    #跳过数据部分
                    next_page_num,next_record_id = struct.unpack_from('<ii',cur_page.page_data,field_offset)
                    if  not (next_page_num,next_record_id) in wait_deleted:
                        wait_deleted.append((next_page_num,next_record_id))
                    field_offset += field_length
                    field_offset += 4 + 4
                else:
                    field_offset += field_length
            #需要调整偏移量
            cur_page.shrink(record_offset+record_length,-record_length)
            #删除slot,将被删除的slot移除到最后的位置
            cur_page.move_and_insert_slot(slot,cur_page.slot_num - 1)
            cur_page.decrease_slot_num()
            if not record_header.next_page_num == -1 and not (record_header.next_page_num,record_header.next_record_id) in wait_deleted:
                wait_deleted.append((record_header.next_page_num,record_header.next_record_id))
