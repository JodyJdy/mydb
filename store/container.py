import config
import struct
from typing import Dict
from store.cacheable import CacheablePage
from store.loggable import Loggable
from store.page import  CommonPage


class ManagementPage(CacheablePage):
    """管理页面结构
    总页面数量: total  4
    空闲页面数量  free 4
    下一个管理页面位置: next_management 4
    日志记录号 8
    """
    def __init__(self,page_num:int,page_data:bytearray):
        super().__init__(page_num, page_data)
        # 每个管理页面64字节：12字节头部 + 52字节位图
        self.total,self.free,self.next_management,self.lsn = struct.unpack_from('<iiiL',page_data,0)
        #未初始化的管理页面，进行初始化
        if self.total == 0:
            self.total = self.free = ManagementPage.capacity()
            self.write_header()  # 初始元数据

    def do_write_header(self,total,free,next_management,lsn):
        """写入管理页面头部信息"""
        struct.pack_into('<iiiL',self.page_data,0, total, free, next_management,lsn)

    def write_header(self):
        self.dirty = True
        self.do_write_header(self.total,self.free,self.next_management,self.lsn)

    def set_bit(self, bit_pos):
        self.dirty = True
        self.free-=1
        self.sync()
        """设置位图中指定位为1"""
        byte_offset = ManagementPage.header_size() + (bit_pos // 8)
        bit_offset = bit_pos % 8
        if byte_offset < config.PAGE_SIZE:
            self.page_data[byte_offset] |= (1 << bit_offset)

    def clear_bit(self, bit_pos):
        self.dirty = True
        self.free+=1
        self.sync()
        """将位图中指定位设置为0"""
        byte_offset =  ManagementPage.header_size() + (bit_pos // 8)
        bit_offset = bit_pos % 8
        if byte_offset <  config.PAGE_SIZE:
            self.page_data[byte_offset] &= ~(1 << bit_offset)

    def is_bit_set(self, bit_pos):
        """检查位图中指定位是否为1"""
        byte_offset = ManagementPage.header_size() + (bit_pos // 8)
        bit_offset = bit_pos % 8
        if byte_offset >= config.PAGE_SIZE:
            return False
        return (self.page_data[byte_offset] & (1 << bit_offset)) != 0

    def sync(self):
        self.write_header()

    def is_free(self)->bool:
        return self.free > 0

    @staticmethod
    def header_size()->int:
        return  4 + 4 + 4 + 8

    @staticmethod
    def capacity():
        """获取管理页面的管理容量（位）"""
        return (config.PAGE_SIZE  - ManagementPage.header_size()) * 8

class LoggableManagement(ManagementPage, Loggable):
    def __init__(self,page_num:int,page_data:bytearray):
        super().__init__(page_num, page_data)
    def clear_bit(self, bit_pos):
        print(f'container_id:{self.container_id}, page_id:{self.page_num} operate:clear_bit: {bit_pos}')
        super().clear_bit(bit_pos)
    def set_bit(self, bit_pos):
        print(f'container_id:{self.container_id}, page_id:{self.page_num} operate:set_bit: {bit_pos}')
        super().set_bit(bit_pos)
    def do_write_header(self,total,free,next_management,lsn):
        print(f'container_id:{self.container_id}, page_id:{self.page_num} operate:write_heaer:{free} {lsn}')
        super().do_write_header(total,free,next_management,lsn)

class PageManager:
    """页面管理系统"""
    def __init__(self,container):
        self.container = container
        self.first_page = container.get_page(0,True)

    @staticmethod
    def next_management_page_num(management_page:ManagementPage)->int:
        """
            计算下一个 管理页码的 真实物理页号
        """
        return (ManagementPage.capacity() +1 ) + management_page.page_num

    def free_page(self,page_num:int):
        pos = page_num % (ManagementPage.capacity() +1 )
        if pos == 0:
            raise Exception('管理页面不允许释放')
        management_page = page_num - pos
        page = self.container.get_page(management_page,True)
        page.clear_bit(pos-1)

    def alloc_page(self):
        management_page,pos = self._allocate_page()
        #计算物理偏移量
        return management_page + pos + 1

    def _allocate_page(self)->[int,int]:
        """分配新页面
        返回 管理页面的 页号 和 偏移量， 真实的 物理页号需要再进行一次计算
        """
        current = self.first_page
        while True:
            if current.is_free():
                # 在当前管理页面寻找空闲位
                for pos in range(ManagementPage.capacity()):
                    if not current.is_bit_set(pos):
                        current.set_bit(pos)
                        return current.page_num,pos
            if current.next_management == 0:
                #计算新管理页码的物理页号
                new_mg_page_num = PageManager.next_management_page_num(current)
                current.next_management =  new_mg_page_num
                current.write_header()
            else:
                new_mg_page_num = current.next_management
            # 需要创建新管理页面
            new_mgmt = self.container.get_page(new_mg_page_num,True)
            current = new_mgmt


class Container:
    def __init__(self, container_name: str | None = None, log:bool = True):
        self.container_name = container_name
        self.file = config.create_container_if_need(container_name)
        self.cache:Dict[int,CacheablePage]={}
        #container的唯一标识符
        self.container_id = config.get_container_id(container_name)
        #统一配置是否记录container页面变更
        self.log = log
        self.page_manager = PageManager(self)

    def get_size(self):
        return config.container_size(self.container_name)

    def seek_page(self, page_number: int):
        offset = page_number * config.PAGE_SIZE
        self.file.seek(offset)

    def seek_offset(self, offset: int):
        self.file.seek(offset)

    def read_page(self, page_number: int, page_data: bytearray):
        cur_eof = self.get_size()
        read_end =(page_number + 1) * config.PAGE_SIZE
        #读取的页是新的页
        if cur_eof < read_end:
            self.pad_file(read_end)
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

    def free_page(self, page_num:int):
        """
        释放页
        :param page_num:
        :return:
        """
        self.page_manager.free_page(page_num)


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

    def get_page(self,page_num:int,management_page:bool = False):
        from store.page import LoggablePage,CommonPage
        if page_num in self.cache:
            return self.cache[page_num]
        page_data = bytearray()
        self.read_page(page_num, page_data)
        if management_page:
            if self.log:
                page = LoggableManagement(page_num, page_data)
            else:
                page = ManagementPage(page_num,page_data)
        else:
            if self.log:
                page = LoggablePage(page_num, page_data)
            else:
                page = CommonPage(page_num, page_data)
        page.set_container(self)
        self.cache[page_num] = page
        return page

    def new_common_page(self,is_over_flow:bool = False):
        from store.page import CommonPage,LoggablePage
        page_data = bytearray(config.PAGE_SIZE)
        page_num = self.page_manager.alloc_page()
        if self.log:
            page = LoggablePage(page_num, page_data)
        else:
            page = CommonPage(page_num,page_data)
        page.set_container(self)
        if is_over_flow:
            page.init_page(is_over_flow=True)
        else:
            page.init_page(is_over_flow=False)
        self.cache[page_num] = page
        return page



    def close(self):
        self.file.close()

    def flush_single_page(self,page:CommonPage):
        self.write_page(page.page_num,page.page_data)
        self.file.flush()

    def flush(self):
        for k, v in self.cache.items():
            if v.dirty:
                self.write_page(k,v.page_data)
        self.file.flush()
