
from store import log_struct



from typing import Tuple, List, Any

import config
import struct
from store.values import Row,  Value
from store.cacheable import CacheablePage,NORMAL_PAGE,OVER_FLOW_PAGE

"""
slot table entry 大小固定
存放record的 offset 和 length
"""
SLOT_TABLE_ENTRY_SIZE = 4 + 4



def cal_slot_entry_offset(slot: int):
    """
    计算 slot entry的偏移量
    :param slot:
    :return:
    """
    return config.PAGE_SIZE - (slot + 1) * SLOT_TABLE_ENTRY_SIZE


class BasePage(CacheablePage):
    def __init__(self, page_num: int, page_data: bytearray):
        super().__init__(page_num, page_data)
        self.slot_num = 0
        self.next_id = 0

    def is_over_flow(self) -> bool:
        pass


    def increase_slot_num(self):
        self.slot_num += 1
        self.sync()

    def set_slot_num(self,num:int):
        self.slot_num = num
        self.sync()

    def decrease_slot_num(self):
        self.slot_num -= 1
        self.sync()

    def header_size(self) -> int:
        pass

    def delete_by_slot(self, slot: int) -> int:
        """
        返回数据所处的record id
        :param slot:
        :return:
        """
        pass

    def delete_by_record_id(self, record_id: int) -> int:
        """
        返回数据所处的slot
        :param record_id:
        :return:
        """
        pass

    def update_by_record_id(self, row: Row, record_id: int):
        pass

    def update_by_slot(self, row: Row, slot: int):
        pass

    def insert_to_last_slot(self, row: Row, record_id: int | None = None):
        """
        所有的新增只要追加在尾部就行， 修改 slot 的映射关系即可
        数据的逻辑顺序由slot决定， record实际在page上面的顺序是不固定的
        :param row:
        :param record_id:
        :return:
        """
        pass

    def insert_slot(self, row: Row, slot: int, record_id: int | None = None):
        pass


    def get_next_record_id(self):
        """
        生成插入记录的record id
        """
        record_id = self.next_id
        self.next_id += 1
        return record_id

    def init_page_header(self, is_over_flow: bool = False):
        """新创建页面时，需要初始化页面头部"""
        pass

    def read_slot_entry(self, slot) -> Tuple[int, int]:
        """
        返回 记录的: offset, length
        :param slot:
        :return:
        """
        offset = cal_slot_entry_offset(slot)
        return log_struct.unpack_from('<ii', self.page_data, offset)

    def cal_free_space(self):
        space_used = self.header_size() + self.slot_num * SLOT_TABLE_ENTRY_SIZE
        for i in range(self.slot_num):
            record_offset, record_len = self.read_slot_entry(i)
            space_used += record_len
        return config.PAGE_SIZE - space_used

    def header_records_length(self, free_space: int):
        """
        计算 page header + 所有record的长度
        :param free_space:
        :return:
        """
        return config.PAGE_SIZE - self.slot_num * SLOT_TABLE_ENTRY_SIZE - free_space

    def shrink(self, offset_start: int, shrink_bytes: int):
        """
        shrink byte，并调整 slot table,以及 record中的长度信息
        :param offset_start: 移动byte的起始位置
        :param shrink_bytes: 移动字节数， 如果 >0 向后移动, 页面可用空间减少
        :return:
        """
        if shrink_bytes == 0:
            return
        # 获取offset_start之后的所有slot
        shrink_slot = []
        for i in range(self.slot_num):
            record_offset, record_len = self.read_slot_entry(i)
            if record_offset >= offset_start:
                # 记录slot num 并调整record偏移
                shrink_slot.append((i, record_offset + shrink_bytes))
        free_space = self.cal_free_space()
        if shrink_bytes > 0 and free_space < shrink_bytes:
            raise Exception('page溢出')
        if shrink_bytes < 0 and shrink_bytes < - offset_start - self.header_size():
            raise Exception('page溢出')
        content_len = self.header_records_length(free_space)
        log_struct.set_page_range_data(self,offset_start + shrink_bytes,content_len + shrink_bytes,
                                       self.page_data[offset_start:content_len]
                                       )

        # 调整slot的偏移量
        for slot, record_offset in shrink_slot:
            self.set_slot_record_offset(slot, record_offset)

    def read_slot(self, slot: int) -> Any:
        """
        根据 slot位置读取数据
        :param slot:
        :return:
        """
        pass

    def read_record(self, record_id: int) -> Any:
        """
        根据record id 读取页的内容
        :param record_id:
        :return:
        """
        pass

    def copy_slot(self, src_slot: int, target_slot: int) -> None:
        """
         将slot table src_slot的内容拷贝到 target_slot
        :param src_slot:
        :param target_slot:
        :return:
        """
        # 读取src_slot的内容
        record_offset, record_len = self.read_slot_entry(src_slot)
        log_struct.pack_into("<ii",self, cal_slot_entry_offset(target_slot), record_offset, record_len)

    def set_slot(self, slot: int, record_offset: int, record_len: int):
        log_struct.pack_into("<ii", self, cal_slot_entry_offset(slot), record_offset, record_len)

    def set_slot_record_offset(self, slot: int, record_offset: int):
        log_struct.pack_into("<i", self, cal_slot_entry_offset(slot), record_offset)

    def search_slot_by_record_offset(self, offset: int):
        for i in range(self.slot_num):
            record_offset, record_len = self.read_slot_entry(i)
            if record_offset == offset:
                return i
        return -1

    def move_and_insert_slot(self, src_slot: int, target_slot: int):
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
        src_record_offset, src_record_len = self.read_slot_entry(src_slot)
        if src_slot > target_slot:
            # 向前移动
            for i in range(src_slot, target_slot, -1):
                self.copy_slot(i - 1, i)
        else:
            # 向后移动
            for i in range(src_slot, target_slot, 1):
                self.copy_slot(i + 1, i)
        self.set_slot(target_slot, src_record_offset, src_record_len)



# record 单页存储完
SINGLE_PAGE = 1
# 多个页存储完
MULTI_PAGE = 0

# 字段的状态
FIELD_NOT_OVER_FLOW = 0
FIELD_OVER_FLOW = 1
FIELD_OVER_FLOW_NULL = 2
FIELD_NOT_OVER_FLOW_NULL = 3


def get_value_status(v: Value):
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
# 1.读取数据
INCLUDE_FIELD_DATA = 0
# 2.只读取字段的头部
ONLY_FIELD_HEADER = 1


class CommonPageRecordHeader:
    def __init__(self, status: int, record_id: int, col_num: int, next_page_num: int, next_record_id: int):
        self.status = status
        self.record_id = record_id
        self.col_num = col_num
        self.next_page_num = next_page_num
        self.next_record_id = next_record_id

    def __repr__(self):
        return f'status={self.status},record_id={self.record_id},col_num={self.col_num},next_page_num={self.next_page_num},next_record_id={self.next_record_id}'


class Field:
    def __init__(self, status: int, page_num: int, offset: int, field_space_use: int,field_data_length:int):
        self.status = status
        # 字段所处的page
        self.page_num = page_num
        # 字段的偏移
        self.offset = offset
        self.field_space_use = field_space_use
        self.field_data_length = field_data_length
        self.value = bytearray()
        self.over_flow_page = None
        self.over_flow_record = None

    def is_null(self):
        return self.status == FIELD_OVER_FLOW_NULL or self.status == FIELD_NOT_OVER_FLOW_NULL

    def is_over_flow(self):
        return self.status == FIELD_OVER_FLOW

    def set_value(self, value: bytearray):
        self.value = value

    def __repr__(self):
        return f'value:{self.value}'


class Record:
    def __init__(self, header: CommonPageRecordHeader, fields: List[Field]):
        self.header = header
        self.fields = fields

    def __repr__(self):
        return f'header:{self.header},fields:{self.fields}'


class CommonPage(BasePage):
    """
        结构:
     是否是 over_flow_page 1
     slot数量  4
     下一个可用的记录id  4
     当前页使用的over_flow_page的最大id 4
     日志记录号 8
     record1,record2,record3 ....
     <slot table>
     record结构: record
      status 1
      record id 4
      page field 在当页的field的数量（如果over flow， 如果没有overflow就是全部的数量）
      over_flow_page number 4
      over_flow_page id 4
      field1,field2,field3 ....
    field结构:  fieldSpace 表示 field在当页除了头部和overflow信息占用的空间, fieldDataLength 则是数据部分的长度 fieldSpace> = fieldDataLength
       和slot table中的 record length含义不一样
        status: 1  字段状态
        如果不over，flow:
        fieldSpace 2  fieldDataLength 2   fieldData
        如果over flow
        fieldSpace 2  fieldDataLength 2  over_flow page_number 4   over_flow_record id 4  fieldData
        如果null,长度依然保留，后续可能会更改内容
     <slot table>
      slot 偏移量  4
      slot 长度

    """

    def __init__(self, page_num: int, page_data: bytearray):
        super().__init__(page_num, page_data)
        self.page_type, self.slot_num, self.next_id, self.over_flow_page_num,self.lsn = log_struct.unpack_from('<biiiL',
                                                                                                  self.page_data, 0)

    def set_lsn(self, lsn):
        super().set_lsn(lsn)
        #写入 page_data 的lsn !!!!! 配合binlog使用，不需要记录日志，此处使用普通的struct方法
        struct.pack_into('<L', self.page_data, self.lsn_offset(),self.lsn)

    def sync(self):
        log_struct.pack_into(CommonPage.header_fmt(), self, 0, self.page_type, self.slot_num, self.next_id,
                         self.over_flow_page_num,self.lsn)
        self.dirty = True

    def lsn_offset(self):
        return struct.calcsize(CommonPage.header_fmt()[0:-1])

    @staticmethod
    def header_fmt():
        return '<biiiL'

    def header_size(self) -> int:
        return struct.calcsize(self.header_fmt())

    @staticmethod
    def over_flow_field_header():
        return CommonPage.field_header_length() + CommonPage.over_flow_field_data_size()

    @staticmethod
    def field_header_length():
        """
         status fieldSpace fieldDataLength
        :return:
        """
        return 1 + 2 + 2

    @staticmethod
    def record_header_fmt():
        return '<biiii'

    @staticmethod
    def record_header_size() -> int:
        """
             record结构:
                  status 1
                  record id 4
                  page field数量 4
                  over_flow_page number 4
                  over_flow_page id 4
        """
        return struct.calcsize(CommonPage.record_header_fmt())

    @staticmethod
    def record_min_size() -> int:
        """
        存储一个记录最少需要的空间
        :return:
        """
        return CommonPage.record_header_size() + SLOT_TABLE_ENTRY_SIZE

    @staticmethod
    def over_flow_field_data_size() -> int:
        """
        不包含 头部信息
        over_page_num 4
        over_record_num 4
        :return:
        """
        return 4 + 4

    def get_over_flow_page(self, min_space: int | None = None):
        if self.over_flow_page_num == -1:
            over_page = self.container.new_common_page(is_over_flow=True)
            self.over_flow_page_num = over_page.page_num
        else:
            over_page = self.container.get_page(self.over_flow_page_num)
        if min_space and over_page.cal_free_space() < min_space:
            over_page = self.container.new_common_page(is_over_flow=True)
            self.over_flow_page_num = over_page.page_num
        return over_page

    def get_slot_num_by_record_id(self, record_id: int):
        for i in range(self.slot_num):
            offset, record_length = self.read_slot_entry(i)
            _, temp_record_id = log_struct.unpack_from('<bi', self.page_data, offset)
            if temp_record_id == record_id:
                return i
        return -1

    def get_record_id_by_slot(self, slot: int):
        offset, _ = self.read_slot_entry(slot)
        _, record_id = log_struct.unpack_from('<bi', self.page_data, offset)
        return record_id

    def read_record_header_by_slot(self, slot: int) -> Tuple[int, int, CommonPageRecordHeader]:
        record_offset, record_length = self.read_slot_entry(slot)
        return record_offset, record_length, CommonPageRecordHeader(
            *log_struct.unpack_from(CommonPage.record_header_fmt(), self.page_data, record_offset))

    def read_record_header_by_record_id(self, record_id: int) -> Tuple[int, int, int, CommonPageRecordHeader]:
        """
        返回 记录偏移，记录长度 slot record_header
        """
        slot = self.get_slot_num_by_record_id(record_id)
        record_offset, record_length = self.read_slot_entry(slot)
        record_header = CommonPageRecordHeader(*log_struct.unpack_from(CommonPage.record_header_fmt(), self.page_data, record_offset))
        return record_offset, record_length, slot, record_header

    def insert_slot(self, row: Row, slot: int, record_id: int | None = None):
        # 插入尾部
        page_num, record_id = self.insert_to_last_slot(row, record_id)
        if page_num == -1:
            return -1, -1
        # 调整slot的位置
        # 交换slot位置
        self.move_and_insert_slot(self.slot_num - 1, slot)
        return page_num,record_id

    def write_long_field(self, field: Value, write_from, over_record_id: int, page: BasePage):
        cur_record_id = over_record_id
        cur_page = page
        field_bytes = field.get_bytes()
        field_size = len(field_bytes)
        # 每次都尝试把页写满
        while write_from < field_size:
            free_space = cur_page.cal_free_space()
            # 还需要写的数据长度
            field_length = field_size - write_from
            # 获取写入数据的
            record_offset_start = cur_page.header_records_length(free_space)
            slot_offset_start = cal_slot_entry_offset(cur_page.slot_num)
            # 调整可用空间
            free_space -= CommonPage.record_min_size()
            # 写入field的位置
            field_write_offset = record_offset_start + cur_page.record_header_size()
            # 写入field
            # 1. 本次可以完全写完
            if field_length + CommonPage.field_header_length() <= free_space:
                # 写入数据部分, field_space_use和field_data_length保持一致
                log_struct.pack_into('<bHH', cur_page, field_write_offset, FIELD_NOT_OVER_FLOW, field_length,field_length)

                log_struct.set_page_range_data(cur_page,
                                               field_write_offset + CommonPage.field_header_length(),
                                               field_write_offset + CommonPage.field_header_length() + field_length,
                                               field.get_bytes()[write_from:]
               )
                # 写入 record header
                log_struct.pack_into(CommonPage.record_header_fmt(), cur_page, record_offset_start, SINGLE_PAGE, cur_record_id, 1, -1,
                                 -1)
                # 写入 slot table
                log_struct.pack_into('<ii', cur_page, slot_offset_start, record_offset_start,
                                 field_write_offset + CommonPage.field_header_length() + field_length - record_offset_start)
                write_from += field_length
                cur_page.increase_slot_num()
                break
            # 写多次
            else:
                cur_page_len = free_space - CommonPage.over_flow_field_header()
                over_page = cur_page.get_over_flow_page(
                    CommonPage.record_min_size() + CommonPage.field_header_length() + min(cur_page_len, 10))
                over_page_record_id = over_page.get_next_record_id()
                log_struct.pack_into('<bHHii', cur_page, field_write_offset, FIELD_OVER_FLOW, cur_page_len,cur_page_len,
                                 over_page.page_num, over_page_record_id)

                log_struct.set_page_range_data(cur_page,
                                               field_write_offset + CommonPage.over_flow_field_header() ,
                                               CommonPage.over_flow_field_header() + field_write_offset + cur_page_len ,
                                               field.get_bytes()[write_from:write_from + cur_page_len]
                )
                write_from += cur_page_len
                # 写入 record header
                log_struct.pack_into(CommonPage.record_header_fmt(), cur_page, record_offset_start, MULTI_PAGE, cur_record_id, 1,
                                 over_page.page_num, over_page_record_id)
                # 写入 slot table
                log_struct.pack_into('<ii', cur_page, slot_offset_start, record_offset_start,
                                 field_write_offset + CommonPage.over_flow_field_header() + cur_page_len - record_offset_start)
                cur_page.increase_slot_num()
                # 多的写入下一页
                cur_page = over_page
                cur_record_id = over_page_record_id

    def write_field(self, field: Value, page, free_space: int, write_offset: int):
        """
        field_length 表示在整个field 占据的空间 包括头部
        field_space_use field全部数据部分占据的空间，存在未使用的情况
        field_data_length  field 使用的数据存放的空间
        例如原先数据是: "hello"
           field_space_use ==  field_data_length = 5
           当 "hello"  -> "hel"是时，并不释放空间
           field_space_use ==  5, field_data_length == 3
        """
        status = get_value_status(field)
        if field.is_null:
            # 预留一个 over flow的空间
            if field.len_variable():
                #over flow null 数据部分长度是0
                field_space_use = field_data_length = 0
                field_length = CommonPage.over_flow_field_data_size()
            else:
                field_space_use = field_data_length = field_length = field.space_use()
            if field_length + CommonPage.field_header_length() > free_space:
                return -1
            # 可以写入
            log_struct.pack_into('<bHH', page, write_offset, status,field_space_use, field_data_length)
            return CommonPage.field_header_length() + field_length
        else:
            field_space_use = field_data_length = field.space_use()
            # 当页全部放得下，且不是over flow数据类型
            if not field.len_variable() and field_data_length + CommonPage.field_header_length() <= free_space:
                # 写入数据部分
                log_struct.pack_into('<bHH', page, write_offset, FIELD_NOT_OVER_FLOW, field_space_use,field_data_length)
                log_struct.set_page_range_data(page,
                                               write_offset + CommonPage.field_header_length(),
                                               write_offset + CommonPage.field_header_length() + field_data_length,
                                               field.get_bytes()
                                               )
                return CommonPage.field_header_length() + field_data_length
            # 当页不能写完,只能写入一部分,或者本身就是over flow 类型，需要over flow
            else:
                # 连over flow空间都没有，直接结束
                if free_space < CommonPage.over_flow_field_header():
                    return -1
                # 当页可以写入的长度
                cur_page_len = free_space - CommonPage.over_flow_field_header()
                if cur_page_len < field_data_length:
                    # 新开辟的页，最少也要有写入一个field的空间
                    over_page = page.get_over_flow_page(
                        CommonPage.record_min_size() + CommonPage.field_header_length() + min(cur_page_len, 10))
                    over_page_record_id = over_page.get_next_record_id()
                    log_struct.pack_into('<bHHii', page, write_offset, FIELD_OVER_FLOW, cur_page_len,cur_page_len,
                                         over_page.page_num, over_page_record_id)

                    log_struct.set_page_range_data(page,
                                                   write_offset + CommonPage.over_flow_field_header(),
                                                   CommonPage.over_flow_field_header() + write_offset + cur_page_len,
                                                   field.get_bytes()[0:cur_page_len]
                    )
                    # 剩余的写入over_page
                    page.write_long_field(field, cur_page_len, over_page_record_id, over_page)
                    # fielder header + over flow信息 + 当页写入信息
                    return cur_page_len + CommonPage.over_flow_field_header()
                else:
                    #要留下over flow 的位置
                    log_struct.pack_into('<bHHii', page, write_offset, FIELD_OVER_FLOW, field_space_use,field_data_length,-1, -1)

                    log_struct.set_page_range_data(page,
                                                   write_offset + CommonPage.over_flow_field_header(),
                                                   CommonPage.over_flow_field_header() + write_offset + field_data_length,
                                                   field.get_bytes()
                                                   )
                    return field_data_length +CommonPage.over_flow_field_header()

    def insert_over_flow_record(self,over_flow_page_num:int,over_flow_record_id:int, record_id:int|None=None)->Tuple[int,int]:
        if not record_id:
            record_id  =  self.get_next_record_id()
        # 可用空间
        free_space = self.cal_free_space()
        # 不够写入header信息
        if free_space < CommonPage.record_min_size():
            return -1, -1
        slot_offset_start = cal_slot_entry_offset(self.slot_num)
        # 获取写入record数据的偏移
        record_offset_start = self.header_records_length(free_space)
        log_struct.pack_into(CommonPage.record_header_fmt(), self, record_offset_start, MULTI_PAGE, record_id,
                         0, over_flow_page_num,
                         over_flow_record_id)
        #只写入头部，什么都不写
        log_struct.pack_into('<ii', self, slot_offset_start, record_offset_start,
                         CommonPage.record_header_size())
        self.increase_slot_num()
        return record_id,self.page_num

    def insert_to_last_slot(self, row: Row, record_id: int | None = None) -> Tuple[int, int]:
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
        # 逐个field的写入
        wrote_cols = 0
        while wrote_cols < len(row.values):
            # 可用空间
            free_space = cur_page.cal_free_space()
            # 不够写入header信息
            if free_space < CommonPage.record_min_size():
                return -1, -1
            # 获取写入数据的
            record_offset_start = cur_page.header_records_length(free_space)

            slot_offset_start = cal_slot_entry_offset(cur_page.slot_num)
            # 调整可用空间
            free_space -= CommonPage.record_min_size()
            # 写入field的位置
            field_write_offset = record_offset_start + cur_page.record_header_size()
            step_wrote_cols = 0
            while wrote_cols + step_wrote_cols < len(row.values):
                value = row.values[wrote_cols + step_wrote_cols]
                write_len = cur_page.write_field(value, cur_page, free_space, field_write_offset)
                if write_len == -1:
                    break
                free_space -= write_len
                field_write_offset += write_len
                step_wrote_cols += 1
            # 当页没有写完,record需要多个页来存储
            if wrote_cols + step_wrote_cols < len(row.values):
                over_page = cur_page.get_over_flow_page(CommonPage.record_min_size())
                over_record_id = over_page.get_next_record_id()
                log_struct.pack_into(CommonPage.record_header_fmt(), cur_page, record_offset_start, MULTI_PAGE, cur_record_id,
                                 step_wrote_cols, over_page.page_num,
                                 over_record_id)
                log_struct.pack_into('<ii', cur_page, slot_offset_start, record_offset_start,
                                 field_write_offset - record_offset_start)
                cur_page.increase_slot_num()
                cur_record_id = over_record_id
                cur_page = over_page
            #当页写完了
            else:
                cur_page.increase_slot_num()
                log_struct.pack_into(CommonPage.record_header_fmt() , cur_page, record_offset_start, SINGLE_PAGE, cur_record_id,
                                 step_wrote_cols, -1,
                                 -1)
                log_struct.pack_into('<ii', cur_page, slot_offset_start, record_offset_start,
                                 field_write_offset - record_offset_start)
            wrote_cols += step_wrote_cols

        return record_id, self.page_num


    def is_over_flow(self) -> bool:
        return self.page_type == OVER_FLOW_PAGE

    def init_page_header(self, is_over_flow: bool = False):
        """
        写入头部信息
        """
        if is_over_flow:
            log_struct.pack_into('<biiiL', self, 0, OVER_FLOW_PAGE, 0, 0, -1,0)
        else:
            log_struct.pack_into('<biiiL', self, 0, NORMAL_PAGE, 0, 0, -1,0)
        self.over_flow_page_num = -1

    def delete_by_slot(self, slot: int) -> int:
        record_id = self.get_record_id_by_slot(slot)
        self.delete_by_record_id(record_id)

    def delete_by_record_id(self, record_id: int):
        self._delete_record_id(record_id)

    def read_record(self, record_id: int) -> Record:
        slot = self.get_slot_num_by_record_id(record_id)
        return self.read_slot(slot)

    def read_over_flow_field(self, next_page_num: int, next_record_id: int, field: Field):
        """
        over field 占用的record只会有一个col
        :param next_page_num:
        :param next_record_id:
        :param field:
        :return:
        """
        cur_page = self.container.get_page(next_page_num)
        cur_slot = cur_page.get_slot_num_by_record_id(next_record_id)
        while True:
            record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)
            field_offset = record_offset + cur_page.record_header_size()
            status, field_space_use,field_data_length = log_struct.unpack_from('<bHH', cur_page.page_data, field_offset)
            field_offset += cur_page.field_header_length()
            if status == FIELD_NOT_OVER_FLOW:
                # 读取结束
                field.value.extend(cur_page.page_data[field_offset:field_offset + field_data_length])
                return
            else:
                temp_next_page_num, temp_next_record_id = log_struct.unpack_from('<ii', cur_page.page_data, field_offset)
                if temp_next_page_num == -1:
                    return
                # 跳过over flow部分
                field_offset += self.over_flow_field_data_size()
                # 读取数据
                field.value.extend(cur_page.page_data[field_offset:field_offset + field_data_length])
                cur_page = self.container.get_page(temp_next_page_num)
                cur_slot = cur_page.get_slot_num_by_record_id(temp_next_record_id)


    def read_all_field(self, record_id: int)->List[Field]:
        """
        读取field的偏移，不读取数据
        """
        cur_slot = self.get_slot_num_by_record_id(record_id)
        cur_page = self
        record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)

        result=  []

        while True:
            # 读取field
            field_offset = record_offset + cur_page.record_header_size()
            # 在当前页，进行读取
            for i in range(record_header.col_num):
                status, field_space_use,field_data_length = log_struct.unpack_from('<bHH', cur_page.page_data, field_offset)
                field = Field(status, cur_page.page_num, field_offset, field_space_use,field_data_length)
                field_offset += cur_page.field_header_length()
                if status == FIELD_OVER_FLOW :
                    next_page_num, next_record_id = log_struct.unpack_from('<ii', cur_page.page_data, field_offset)
                    field.over_flow_page = next_page_num
                    field.over_flow_record = next_record_id
                    # 跳过over flow部分
                    field_offset += self.over_flow_field_data_size()
                    field_offset += field_space_use
                elif status == FIELD_OVER_FLOW_NULL:
                    field_offset += self.over_flow_field_data_size()
                    field_offset += field_space_use
                else:
                    field_offset += field_space_use
                # 读取到field头部
                result.append(field)
            # 读取下一页
            if  record_header.next_page_num != -1:
                cur_page = self.container.get_page(record_header.next_page_num)
                cur_slot = cur_page.get_slot_num_by_record_id(record_header.next_record_id)
                record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)
            else:
                break
        return result



    def read_field_by_index(self, record_id: int, field_index: int):
        """
        读取field的偏移，不读取数据
        """
        cur_slot = self.get_slot_num_by_record_id(record_id)
        cur_page = self
        record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)
        index = 0
        while True:
            # 读取field
            field_offset = record_offset + cur_page.record_header_size()
            # 在当前页，进行读取
            if index + record_header.col_num > field_index:
                for i in range(record_header.col_num):
                    status, field_space_use,field_data_length = log_struct.unpack_from('<bHH', cur_page.page_data, field_offset)
                    field = Field(status, cur_page.page_num, field_offset, field_space_use,field_data_length)
                    field_offset += cur_page.field_header_length()
                    if status == FIELD_OVER_FLOW :
                        next_page_num, next_record_id = log_struct.unpack_from('<ii', cur_page.page_data, field_offset)
                        field.over_flow_page = next_page_num
                        field.over_flow_record = next_record_id
                        # 跳过over flow部分
                        field_offset += self.over_flow_field_data_size()
                        field_offset += field_space_use
                    elif status == FIELD_OVER_FLOW_NULL:
                        field_offset += self.over_flow_field_data_size()
                        field_offset += field_space_use
                    else:
                        field_offset += field_space_use
                    # 读取到field头部
                    if index + i == field_index:
                        return field
            # 读取下一页
            index += record_header.col_num
            # 读取下一页
            if  record_header.next_page_num != -1:
                cur_page = self.container.get_page(record_header.next_page_num)
                cur_slot = cur_page.get_slot_num_by_record_id(record_header.next_record_id)
                record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)
            else:
                break

    def update_slot_field_by_index(self,slot:int, field_index: int, value: Value):
        record_id = self.get_record_id_by_slot(slot)
        self.update_field_by_index(record_id, field_index,value)
    def update_field_by_index(self, record_id: int, field_index: int, value: Value):
        field = self.read_field_by_index(record_id, field_index)
        self.update_field(field, value)

    def update_field(self, field: Field, value: Value):
        page = self.container.get_page(field.page_num)
        if value.is_null:
            if field.is_null():
                return
            # 如果是over flow不删除当页数据，空间继续占据
            if field.status == FIELD_OVER_FLOW:
                # page.shrink(field.offset + field.field_length + CommonPage.over_flow_field_header(),
                #             -field.field_length)
                # 删除后续数据
                if  field.over_flow_page != -1:
                    self.container.get_page(field.over_flow_page).delete_by_record_id(field.over_flow_record)
                # field_space_use 不变，field_data_length 设置为0
                log_struct.pack_into('<bHHii', page, field.offset, FIELD_OVER_FLOW_NULL,field.field_space_use,0,-1,-1)
            else:
                log_struct.pack_into('<b', page, field.offset, FIELD_NOT_OVER_FLOW_NULL)
            return
        field_data_length = value.space_use()
        if field.is_null():
            # 预留了over_flow的空间，直接写入
            if value.len_variable():
                #查看有无空间写入，如果有，当页能写多少写多少
                #已有空间可以全部写入
                if field.field_space_use >= field_data_length:
                    log_struct.set_page_range_data(page,
                                                   field.offset + CommonPage.over_flow_field_header(),
                                                   field.offset + CommonPage.over_flow_field_header() + field.field_space_use,
                                                   value.get_bytes()[0: field_data_length]
                                                   )
                    #这里 field_space_use和field_data_length保持一致
                    log_struct.pack_into('<bHHii', page, field.offset, FIELD_OVER_FLOW, field.field_space_use,field_data_length, -1,
                                         -1)
                else:
                    #写入一部分，剩余的over flow
                    log_struct.set_page_range_data(page,
                                                   field.offset + CommonPage.over_flow_field_header(),
                                                   field.offset + CommonPage.over_flow_field_header() + field.field_space_use,
                                                   value.get_bytes()[0: field.field_space_use]
                                                   )
                    over_flow_page = self.get_over_flow_page(
                        CommonPage.record_min_size() + CommonPage.over_flow_field_header())
                    over_flow_record_id = over_flow_page.get_next_record_id()

                    #这里 field_space_use和field_data_length保持一致
                    log_struct.pack_into('<bHHii', page, field.offset, FIELD_OVER_FLOW, field.field_space_use,field.field_space_use, over_flow_page.page_num,
                                     over_flow_record_id)
                    #写入位置从 field.field_space_use开始
                    page.write_long_field(value, field.field_space_use, over_flow_record_id, over_flow_page)
            # 长度固定，直接写数据
            else:
                log_struct.pack_into('<bHH', page, field.offset, FIELD_NOT_OVER_FLOW,field_data_length,field_data_length)
                log_struct.set_page_range_data(page,
                                               field.offset + CommonPage.field_header_length(),
                                               field.offset + CommonPage.field_header_length() + field_data_length,
                                               value.get_bytes()
               )
            return

        # 所有值为空的情况都已经处理
        # 可以在当页放下
        if field_data_length <= field.field_space_use:
            if field.status == FIELD_OVER_FLOW:
                # 删除多余的内容; FIELD_OVER_FLOW会预留over_flow_page,over_flow_record，但是不一定使用到
                #所以会有 over_flow_page为-1的情况
                if field.over_flow_page and field.over_flow_page != -1:
                    self.container.get_page(field.over_flow_page).delete_by_record_id(field.over_flow_record)

                log_struct.set_page_range_data(page,
                                               field.offset + CommonPage.over_flow_field_header(),
                                               field.offset + CommonPage.over_flow_field_header() + field_data_length,
                                               value.get_bytes()
                                               )
                log_struct.pack_into('<bHHii', page, field.offset, FIELD_OVER_FLOW,field.field_space_use,field_data_length,-1,-1)

            else:
                log_struct.set_page_range_data(page,
                                               field.offset + CommonPage.field_header_length(),
                                               field.offset + CommonPage.field_header_length() + field_data_length,
                                               value.get_bytes()
                                               )
                log_struct.pack_into('<bHH', page, field.offset, FIELD_NOT_OVER_FLOW,field.field_space_use,field_data_length)
            return
        # 不能在当页放下，一定是 over flow,(包括长度不可变的数据) 能放多少放多少
        log_struct.set_page_range_data(page,
                                       field.offset + CommonPage.over_flow_field_header(),
                                       field.offset + CommonPage.over_flow_field_header() + field.field_space_use,
                                       value.get_bytes()[:field.field_space_use]
       )
        # 删除field over flow多余部分，重新写入
        if  field.over_flow_page and  field.over_flow_page != -1:
            self.container.get_page(field.over_flow_page).delete_by_record_id(field.over_flow_record)
        over_flow_page = self.get_over_flow_page(CommonPage.record_min_size() + CommonPage.over_flow_field_header())
        over_flow_record_id = over_flow_page.get_next_record_id()
        log_struct.pack_into('<bHHii', page, field.offset, FIELD_OVER_FLOW, field.field_space_use,field.field_space_use, over_flow_page.page_num,
                         over_flow_record_id)
        page.write_long_field(value, field.field_space_use, over_flow_record_id, over_flow_page)

    def update_by_record_id(self, row: Row, record_id: int):
        #! !!!! 一定要反向更新，因为每个field在调整时会shrink data，如果正向更新，后续的field的offset会
        #变化
        for field,value in zip(reversed(self.read_all_field(record_id)),reversed(row.values)):
            self.update_field(field, value)
        # for i, value in enumerate(row.values):
        #     self.update_field_by_index(record_id, i, value)


    def update_by_slot(self, row: Row, slot: int):
        record_id = self.get_record_id_by_slot(slot)
        self.update_by_record_id(row, record_id)

    def read_slot(self, slot: int) -> Record:
        cur_slot = slot
        cur_page = self
        record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)
        result_record_header = record_header
        fields_list = []
        while True:
            # 读取field
            field_offset = record_offset + cur_page.record_header_size()
            for i in range(record_header.col_num):
                status, field_space_use,field_data_length = log_struct.unpack_from('<bHH', cur_page.page_data, field_offset)
                field = Field(status, cur_page.page_num, field_offset, field_space_use,field_data_length)
                fields_list.append(field)
                field_offset += cur_page.field_header_length()
                if status == FIELD_NOT_OVER_FLOW_NULL :
                    field_offset += field_space_use
                elif status == FIELD_OVER_FLOW_NULL:
                    field_offset += self.over_flow_field_data_size()
                    field_offset += field_space_use
                elif status == FIELD_NOT_OVER_FLOW:
                    # 读取数据
                    field.value.extend(cur_page.page_data[field_offset:field_offset + field_data_length])
                    field_offset += field_space_use
                else:
                    next_page_num, next_record_id = log_struct.unpack_from('<ii', cur_page.page_data, field_offset)
                    # 跳过over flow部分
                    field_offset += self.over_flow_field_data_size()
                    # 读取数据
                    field.value.extend(cur_page.page_data[field_offset:field_offset + field_data_length])
                    field_offset += field_space_use
                    if  next_page_num != -1:
                        cur_page.read_over_flow_field(next_page_num, next_record_id, field)
            # 读取下一页
            if  record_header.next_page_num != -1:
                cur_page = self.container.get_page(record_header.next_page_num)
                cur_slot = cur_page.get_slot_num_by_record_id(record_header.next_record_id)
                record_offset, _, record_header = cur_page.read_record_header_by_slot(cur_slot)
            else:
                break
        return Record(result_record_header, fields_list)

    def _delete_record_id(self, record_id: int):
        """
        :return:
        """
        deleted = []
        wait_deleted = [(self.page_num, record_id)]
        while len(wait_deleted) > 0:
            cur_page_num, cur_record_id = wait_deleted.pop(0)
            if (cur_page_num, record_id) in deleted:
                continue
            deleted.append((cur_page_num, cur_record_id))
            cur_page = self.container.get_page(cur_page_num)
            record_offset, record_length, slot, record_header = cur_page.read_record_header_by_record_id(cur_record_id)
            if record_offset is None:
                return
            #覆盖 record 的状态 over flow 状态
            log_struct.pack_into(CommonPage.record_header_fmt(),cur_page,record_offset,0,0,0,-1,-1)
            # 读取field
            field_offset = record_offset + cur_page.record_header_size()
            for i in range(record_header.col_num):
                status, field_space_use,field_data_length = log_struct.unpack_from('<bHH', cur_page.page_data, field_offset)
                field_offset += cur_page.field_header_length()
                if status == FIELD_OVER_FLOW:
                    # 跳过数据部分
                    next_page_num, next_record_id = log_struct.unpack_from('<ii', cur_page.page_data, field_offset)
                    if next_page_num != -1:
                        if  (next_page_num, next_record_id) not in wait_deleted:
                            wait_deleted.append((next_page_num, next_record_id))
                    field_offset += field_space_use
                    field_offset += 4 + 4
                elif status == FIELD_OVER_FLOW_NULL:
                    field_offset+=CommonPage.over_flow_field_data_size()
                else:
                    field_offset += field_space_use
            # 需要调整偏移量
            cur_page.shrink(record_offset + record_length, -record_length)
            # 删除slot,将被删除的slot移除到最后的位置
            cur_page.move_and_insert_slot(slot, cur_page.slot_num - 1)
            cur_page.decrease_slot_num()
            if  record_header.next_page_num != -1 and  (record_header.next_page_num,
                                                              record_header.next_record_id) not in wait_deleted:
                wait_deleted.append((record_header.next_page_num, record_header.next_record_id))

    def move_single_slot_to_another_page(self,src_page_slot:int,dst_page_slot:int,another_page):
        record_offset,record_len = self.read_slot_entry(src_page_slot)
        free_space = another_page.cal_free_space()
        new_record_offset_start = another_page.header_records_length(free_space)
        new_slot_offset_start = cal_slot_entry_offset(another_page.slot_num)
        if new_record_offset_start + record_len >= new_slot_offset_start:
            raise Exception('another_page 没有足够的空间')
        #拷贝数据
        log_struct.set_page_range_data(another_page,
                                       new_record_offset_start,
                                       new_record_offset_start+record_len,
                                       self.page_data[record_offset:record_offset+record_len]
        )
        new_record_id = another_page.get_next_record_id()
        log_struct.pack_into('<i',another_page, new_record_offset_start+1, new_record_id)
        #编辑slot table
        log_struct.pack_into('<ii', another_page, new_slot_offset_start, new_record_offset_start,
                         record_len)
        another_page.increase_slot_num()
        #将another 新插入的，调整到指定的位置
        another_page.move_and_insert_slot(another_page.slot_num-1, dst_page_slot)
        #移除数据
        self.shrink(record_offset+record_len,-record_len)
        #移除src_page_slot
        self.move_and_insert_slot(src_page_slot,self.slot_num - 1)
        self.decrease_slot_num()

    def move_to_another_page(self,src_slot:int, dst_slot:int,another_page):
        """
        移动当前页的 [src_slot,dst_slot)到 another_page的尾部
        :param src_slot:
        :param dst_slot:
        :param another_page: 移动到页
        :return:
        """
        another_page:CommonPage
        """不包含 dst_slot"""
        if src_slot >= self.slot_num:
            return
        all_record = []

        for i in range(src_slot,dst_slot):
            record_offset,record_len = self.read_slot_entry(i)
            all_record.append((record_offset,record_len))

        for record_offset,record_len in all_record:
            free_space = another_page.cal_free_space()
            new_record_offset_start = another_page.header_records_length(free_space)
            new_slot_offset_start = cal_slot_entry_offset(another_page.slot_num)
            if new_record_offset_start + record_len > new_slot_offset_start:
                raise Exception('another_page 没有足够的空间')
            #拷贝数据
            log_struct.set_page_range_data(another_page,
                                           new_record_offset_start,
                                           new_record_offset_start+record_len,
                                           self.page_data[record_offset:record_offset+record_len]
            )
            # !!!!!!!!!!!!!!record id 需要重新生成，不能和another_page中的有冲突
            new_record_id = another_page.get_next_record_id()
            log_struct.pack_into('<i',another_page, new_record_offset_start+1, new_record_id)

            #编辑slot table
            log_struct.pack_into('<ii', another_page, new_slot_offset_start, new_record_offset_start,
                             record_len)
            another_page.increase_slot_num()
        #按照偏移量调整，从尾部开始
        all_record.sort(key=lambda x:x[0],reverse=True)
        #移除数据
        for record_offset,record_len in all_record:
            self.shrink(record_offset+record_len,-record_len)
        #移除slot
        for i in range(dst_slot-1,src_slot-1,-1):
            self.move_and_insert_slot(i,self.slot_num - 1)
            self.decrease_slot_num()