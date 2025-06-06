import math
import struct
import typing
from abc import abstractmethod

from typing import List, Any, Dict, Callable

class Value:
    def __init__(self):
        self.is_null = False
        self.bytes_content = None
        self.value = None
    @abstractmethod
    def len_variable(self):
        """长度是否可变"""
        pass
    @abstractmethod
    def space_use(self):
        pass

    def __ne__(self, other):
        return not self.value == other.value

    def __eq__(self, __value):
        return self.value == __value.value

    def __ge__(self, __value):
        return self.value >= __value.value

    def __le__(self, __value):
        return self.value <= __value.value

    def __gt__(self, other):
        return self.value > other.value

    def __lt__(self, other):
        return self.value < other.value

    @abstractmethod
    def get_bytes(self)->bytearray:
        pass
class ByteArray(Value):
    def __init__(self,value:bytearray,is_null:bool=False):
        super().__init__()
        self.value = value
        self.is_null = is_null

    def len_variable(self):
        return True

    def space_use(self):
        return len(self.value)

    def get_bytes(self) -> bytearray:
        return self.value
    def __repr__(self):
        return f"bytearray:{self.value}"
    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return ByteArray(value)

class StrValue(Value):
    def __init__(self,value:str):
        super().__init__()
        self.value = value
        self.is_null = value is None

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        if not value:
            return None
        return StrValue(value.decode('utf8'))

    def len_variable(self):
        return True

    def init_result(self):
        self.bytes_content = bytearray()
        if self.value:
            self.bytes_content.extend(self.value.encode('utf-8'))

    def space_use(self):
        if not self.bytes_content:
            self.init_result()
        return len(self.bytes_content)

    def get_bytes(self) -> bytearray:
        if not self.bytes_content:
            self.init_result()
        return self.bytes_content

    def __repr__(self):
        return f"str:{self.value}"

def int_to_bytes(num,size):
    if num == 0:
        return b'\x00' * size
    # size = math.ceil(num.bit_length() / 8)
    return num.to_bytes(size, byteorder='little', signed=True)

class ShortValue(Value):
    """
        2 字节
    """
    def len_variable(self):
        return False
    def space_use(self):
        return 2
    def get_bytes(self) -> bytearray:
        return self.bytes_content
    def __init__(self,value:int,is_null:bool=False):
        super().__init__()
        self.value = value
        self.is_null = is_null
        if not is_null:
            self.bytes_content = int_to_bytes(value,2)

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return IntValue(int.from_bytes(value, byteorder='little', signed=True))

    def __repr__(self):
        return f"short:{self.value}"


class IntValue(Value):
    """
        4 字节
    """
    def len_variable(self):
        return False
    def space_use(self):
        return 4
    def get_bytes(self) -> bytearray:
        return self.bytes_content
    def __init__(self,value:int,is_null:bool=False):
        super().__init__()
        self.value = value
        self.is_null = is_null
        if not is_null:
            self.bytes_content = int_to_bytes(value,4)

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return IntValue(int.from_bytes(value, byteorder='little', signed=True))

    def __repr__(self):
        return f"int:{self.value}"

class LongValue(Value):
    """
        8 字节
    """
    def len_variable(self):
        return False
    def space_use(self):
        return 8
    def get_bytes(self) -> bytearray:
        return self.bytes_content
    def __init__(self,value:int,is_null:bool=False):
        super().__init__()
        self.value = value
        self.is_null = is_null
        if not is_null:
            self.bytes_content = int_to_bytes(value,8)
    def __repr__(self):
        return f'long:{self.value}'

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return LongValue(int.from_bytes(value, byteorder='little', signed=True))

class Row:
    """
    一个数据行里面的索引总是出现在前几列
    """
    def __init__(self,values:List[Value]):
        self.values = values
        self.space_use = self._space_use()

    def _space_use(self):
        use = 0
        for value in self.values:
            use += value.space_use()
        return use
    def sub_row(self,start:int):
        return Row(self.values[start:])
    def __repr__(self):
        return str(self.values)

    def __ne__(self, other):
        for i in range(len(self.values)):
            v1 = self.values[i]
            v2 = other.values[i]
            if v1 != v2:
                return True
        return False

    def __eq__(self, __value):
        for v1,v2 in zip(self.values,__value.values):
            if v1 != v2:
                return False
        return True

    def __ge__(self, __value):
        for v1,v2 in zip(self.values,__value.values):
            if v1 > v2:
                return True
            elif v1 < v2:
                return False
        return True

    def __le__(self, __value):
        for v1,v2 in zip(self.values,__value.values):
            if v1 < v2:
                return True
            elif v1 > v2:
                return False
        return True

    def __gt__(self, other):
        for v1,v2 in zip(self.values,other.values):
            if v1 > v2:
                return True
        return False

    def __lt__(self, other):
        for v1,v2 in zip(self.values,other.values):
            if v1 < v2:
                return True
        return False


def over_flow_row(v: bytearray):
    return Row([ByteArray(v)])

def generate_row(v:List[int|str|bytearray])->Row:
    values: List[Value] = []
    for value in v:
        if type(value) == int:
            values.append(IntValue(value))
        elif type(value) == str:
            values.append(StrValue(value))
    return Row(values)
"""
 根据类型，生成对应的值
"""
_value_type_dict:Dict[typing.Type[Value],Callable[[bytearray],Value]] = {
    StrValue: StrValue.from_bytes,
    IntValue: IntValue.from_bytes,
    LongValue: LongValue.from_bytes,
    ByteArray: ByteArray.from_bytes,
}

def parse_record(col_types:List[typing.Type[Value]],record)->Row:
    from page import Record
    values = []
    for col_type,field in zip(col_types,record.fields):
        values.append(_value_type_dict[col_type](field.value))
    return Row(values)



