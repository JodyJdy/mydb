import struct
from store.cacheable import CacheablePage

def unpack_from(fmt, buffer, offset=0):
    return struct.unpack_from(fmt,buffer,offset)

def pack_into(fmt,page:CacheablePage, offset, *args):
    return struct.pack_into(fmt,page.page_data,offset,*args)



