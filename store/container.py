
import config
import file_util
import os
import struct
from typing import Dict, Tuple

from store.page import  CommonPage

# 每个extent管理的页的数量
PER_EXTENT_PAGE_NUM = 1024

class Extent:
    #分区 未满
    EXTENT_UN_FILL = 0
    #分区 已满
    EXTENT_FILL = 1
    #page 未使用
    PAGE_NOT_USE = 0
    #page 已经使用
    PAGE_USED = 1
    INVALID_PAGE_NUMBER = -1

    def __init__(self, extent_start_page_num: int, new_page_num: int, used_num: int, status, free_status: bytearray, dirty: bool = False):
        self.extent_start_page_num = extent_start_page_num
        self.new_page_num = new_page_num
        #已经分配的页的数量
        self.used_num = used_num
        self.status = status
        self.free_status = free_status
        self.dirty = dirty

    def has_new_page(self):
        return self.new_page_num != PER_EXTENT_PAGE_NUM

    def free_page(self, page_num: int):
        self.set_page_status(page_num, Extent.PAGE_NOT_USE)

    def is_free(self,page_num:int):
        return self.free_status[page_num] == Extent.PAGE_NOT_USE

    def set_page_status(self, page_num: int, status: int):
        #状态发生变更
        if status != self.free_status[page_num]:
            if status == Extent.PAGE_USED:
                self.used_num+=1
            else:
                self.used_num-=1
        self.free_status[page_num] = status

    def _alloc_local_page(self)->int:
        # 检查有无空闲
        if self.used_num < PER_EXTENT_PAGE_NUM:
            i = 0
            while i < self.new_page_num:
                if self.free_status[i] == Extent.PAGE_NOT_USE:
                    self.set_page_status(i, Extent.PAGE_USED)
                    return i
                i += 1
        # 无空闲页面重新创建
        local_page_num = self._alloc_local_new_page()
        if local_page_num == Extent.INVALID_PAGE_NUMBER:
            self.status = Extent.EXTENT_FILL
        return local_page_num

    def alloc_page(self) -> int:
        if self.status == Extent.EXTENT_FILL:
            return Extent.INVALID_PAGE_NUMBER
        local_page_num = self._alloc_local_page()
        if local_page_num == Extent.INVALID_PAGE_NUMBER:
            return Extent.INVALID_PAGE_NUMBER
        return self.extent_start_page_num + local_page_num

    def _alloc_local_new_page(self):
        """
       生成 extent内的页码
        :return:
        """
        if not self.has_new_page():
            return Extent.INVALID_PAGE_NUMBER
        self.dirty = True
        local_page_num = self.new_page_num
        self.set_page_status(local_page_num, Extent.PAGE_USED)
        self.new_page_num += 1
        return local_page_num

    def alloc_new_page(self) -> int:
        if self.has_new_page():
            #生成全局的 page 编号
            return self.extent_start_page_num + self._alloc_local_new_page()
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

    def is_free(self,page_num: int):
        extent = page_num // PER_EXTENT_PAGE_NUM
        # 不存在
        if extent > self.extent_num:
            return True
        return self.get_extent(extent).is_free(page_num % PER_EXTENT_PAGE_NUM)

    def free_page(self, page_num: int):
        extent = page_num // PER_EXTENT_PAGE_NUM
        # 不存在
        if extent > self.extent_num:
            return
        self.get_extent(extent).free_page(page_num % PER_EXTENT_PAGE_NUM)

    def create_extent(self, i: int) -> Extent:
        extent_start_page_num = i * PER_EXTENT_PAGE_NUM
        new_page_num = 0
        used_num =  0
        status = Extent.EXTENT_UN_FILL
        free_status = bytearray(PER_EXTENT_PAGE_NUM)
        offset = i * ContainerAlloc.extent_space_size() + ContainerAlloc.HEADER_SIZE
        self.file.seek(offset)
        self.file.write(struct.pack('<iiii', extent_start_page_num, new_page_num, used_num, status))
        self.file.write(free_status)
        return Extent(extent_start_page_num, new_page_num, used_num, status, free_status, False)

    @staticmethod
    def extent_space_size() -> int:
        """一个extent占据空间的大小"""
        return ContainerAlloc.EXTENT_HEADER_SIZE + PER_EXTENT_PAGE_NUM

    def read_extent(self, i: int):
        # 读取extent偏移量
        offset = i * ContainerAlloc.extent_space_size() + ContainerAlloc.HEADER_SIZE
        self.file.seek(offset)
        free_status = bytearray()
        free_status.extend(self.file.read(ContainerAlloc.extent_space_size()))
        extent_start_page_num, new_page_num, used_num, status = struct.unpack('<iiii', free_status[0:ContainerAlloc.EXTENT_HEADER_SIZE])
        return Extent(extent_start_page_num, new_page_num, used_num, status, free_status[ContainerAlloc.EXTENT_HEADER_SIZE:])

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
            if extent.has_new_page():
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
                self.file.write(struct.pack('<iiii', e.extent_start_page_num, e.new_page_num, e.length, e.status))
                self.file.write(e.free_status)
        self.file.flush()

    def close(self):
        self.file.close()


class Container:
    def __init__(self, path: str | None = None):
        from page import BasePage
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

    def free_page(self, page_num:int):
        """
        释放页
        :param page_num:
        :return:
        """
        self.alloc.free_page(page_num)


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
        page = CommonPage(page_num, page_data)
        page.set_container(self)
        self.cache[page_num] = page
        return page

    def new_common_page(self,is_over_flow:bool = False):
        from page import CommonPage
        page_data = bytearray(config.PAGE_SIZE)
        page_num = self.alloc.alloc()
        page = CommonPage(page_num, page_data)
        page.set_container(self)
        if is_over_flow:
            page.init_page(is_over_flow=True)
        else:
            page.init_page(is_over_flow=False)
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
