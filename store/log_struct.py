import struct

import config
from store.cacheable import CacheablePage
from store.loggable import Loggable
from collections.abc import Iterable,Sized
from store.log.binlog import *


def unpack_from(fmt, buffer, offset=0):
    return struct.unpack_from(fmt,buffer,offset)

def pack_into(fmt,page:CacheablePage, offset, *args):
    # 打包的二进制数据
    pack_data = struct.pack(fmt,*args)
    set_page_data(page, offset, pack_data)

if config.OPEN_BINLOG:

    def set_page_data(page:CacheablePage, offset, data):
        if page.container.log:
            logentry = PhysicalPageLogEntry(page.container_id,page.page_num,offset,data)
            binlog.write_log_entry(logentry)
        if isinstance(data, Sized):
            page.page_data[offset:offset + len(data)] = data
        else:
            page.page_data[offset] = data
        page.dirty = True
else:
    def set_page_data(page:CacheablePage, offset, data):
        if isinstance(data, Sized):
            page.page_data[offset:offset + len(data)] = data
        else:
            page.page_data[offset] = data
        page.dirty = True

def set_page_range_data(page:CacheablePage,src,dst,data):
    set_page_data(page, src, data)

def set_page_single_byte(page:CacheablePage,pos,data):
    set_page_data(page,pos,data)




