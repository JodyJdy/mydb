import struct

from typing import List, Any


class ByteArray:
    def __init__(self):
        self.array = bytearray()
        self.total_size = 0
        self.all_fmt = ''
        self.write_obj:[List[Any]] = []
    def write_bool(self,b: bool):
        fmt = '?'
        self.all_fmt += fmt
        self.total_size += struct.calcsize(fmt)
        self.write_obj.append(b)
    def write_long(self,i:int):
        fmt = 'l'
        self.all_fmt += fmt
        self.total_size += struct.calcsize(fmt)
        self.write_obj.append(i)
    def write_int(self,i:int):
        fmt = 'i'
        self.all_fmt += fmt
        self.total_size += struct.calcsize(fmt)
        self.write_obj.append(i)
    def write_float(self,f:float):
        fmt = 'f'
        self.all_fmt += fmt
        self.total_size += struct.calcsize(fmt)
        self.write_obj.append(f)
    def write_string(self,s:str):
        b = s.encode('utf-8')
        fmt = f'{len(b)}s'
        self.all_fmt += fmt
        self.total_size += struct.calcsize(fmt)
        self.write_obj.append(b)
    def write_int_array(self,i:List[int]):
        fmt = f'{len(i)}i'
        self.all_fmt += fmt
        self.total_size += struct.calcsize(fmt)
        self.write_obj.append(i)
    def reuse(self):
        self.all_fmt = ''
        self.array = bytearray()
        self.total_size = 0
        self.write_obj = []
    def close_write(self):
        self.array.extend(struct.pack(self.all_fmt,*self.write_obj))
    def total_size(self):
        return self.total_size
    def read(self):
        return struct.unpack(self.all_fmt,self.array)


b = struct.pack('=?ii',True,1,1)
print(b)
print(struct.unpack_from('=ii',b,1))