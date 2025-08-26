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


    @staticmethod
    def type_enum()->int:
        """
        类型的枚举值
        :return:
        """
        pass

    @staticmethod
    def from_bytes(bytes: bytearray):
        pass
    @staticmethod
    def none():
        pass
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

    @staticmethod
    def type_enum() -> int:
        return 0


class StrValue(Value):

    def __init__(self,value:str|None):
        super().__init__()
        self.value = value
        self.is_null = value is None

    @staticmethod
    def none():
        return StrValue(None)

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        if not value:
            return None
        return StrValue(value.decode('utf8'))
    @staticmethod
    def type_enum() -> int:
        return 1

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
    def none():
        return ShortValue(None,is_null=True)

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return IntValue(int.from_bytes(value, byteorder='little', signed=True))

    @staticmethod
    def type_enum() -> int:
        return 2
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
    def type_enum() -> int:
        return 3

    @staticmethod
    def none():
        return IntValue(None,is_null=True)
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
    def type_enum() -> int:
        return 4
    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return LongValue(int.from_bytes(value, byteorder='little', signed=True))
    @staticmethod
    def none():
        return LongValue(None,is_null=True)

class BoolValue(Value):
    """
        8 字节
    """
    def len_variable(self):
        return False
    def space_use(self):
        return 1
    def get_bytes(self) -> bytearray:
        return self.bytes_content
    def __init__(self,value:bool,is_null:bool=False):
        super().__init__()
        self.value = value
        self.is_null = is_null
        if not is_null:
            self.bytes_content =  bytearray()
            self.bytes_content.extend(struct.pack('<b',self.value))
    def __repr__(self):
        return f'long:{self.value}'

    @staticmethod
    def type_enum() -> int:
        return 5

    @staticmethod
    def from_bytes(value:bytearray)->Value|None:
        return BoolValue(struct.unpack('<b',value)[0] == 1)
    @staticmethod
    def none():
        return BoolValue(None,is_null=True)


class Row:
    """
    一个数据行里面的索引总是出现在前几列
    """
    def __init__(self,values:List[Value]):
        self.values = values
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

def any_to_value(v:any):
   if isinstance(v,Value):
       return v
   elif type(v) == int:
       return IntValue(v)
   elif type(v) == str:
       return StrValue(v)
   elif type(v) == bytearray:
       return ByteArray(v)
   elif type(v) == bool:
       return BoolValue(v)
   else:
       raise Exception('不支持的 类型')


def generate_row(v:List[int|str|bytearray])->Row:
    values: List[Value] = []
    for value in v:
        values.append(any_to_value(value))
    return Row(values)


value_type_dict: Dict[int, typing.Type[Value]] = {
    ByteArray.type_enum(): ByteArray,
    StrValue.type_enum(): StrValue,
    ShortValue.type_enum(): ShortValue,
    IntValue.type_enum(): IntValue,
    LongValue.type_enum(): LongValue,
    BoolValue.type_enum(): BoolValue,
}

def serialization_value(v:Value)->bytearray:
    """
    序列化 value 为 byte array
    """
    result =  bytearray()
    result.extend(struct.pack('<i',v.type_enum()))
    result.extend(v.get_bytes())
    return result

def deserialization_value(v:bytearray)->Value|None:
    value_type = struct.unpack('<i',v[0:4])[0]
    return value_type_dict[value_type].from_bytes(v[4:])

def serialization_row(row:Row)->bytearray:
    """
     序列化 row 为 bytearray
    :param row:
    :return:
    """
    size = len(row.values)
    result =  bytearray()
    result.extend(struct.pack('<i',size))
    for value in row.values:
        value_serial = serialization_value(value)
        result.extend(struct.pack('<i',len(value_serial)))
        result.extend(value_serial)
    return result

def deserialization_row(v:bytearray)->Row|None:
    size = struct.unpack('<i',v[0:4])[0]
    value_list = []
    pos = 4
    for i in range(size):
        value_len = struct.unpack('<i',v[pos:pos+4])[0]
        pos += 4
        value  = deserialization_value(v[pos:pos+value_len])
        pos += value_len
        value_list.append(value)
    return Row(value_list)