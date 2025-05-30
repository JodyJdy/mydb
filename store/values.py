import struct
from abc import abstractmethod

from typing import List


class Value:
    def __init__(self):
        self.is_null = False
    @abstractmethod
    def len_variable(self):
        """长度是否可变"""
        pass
    @abstractmethod
    def space_use(self):
        pass

    @abstractmethod
    def get_bytes(self)->bytearray:
        pass
class ByteArray(Value):
    def __init__(self,value:bytearray,is_null:bool=False):
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

class StrValue(Value):
    def __init__(self,value:str,is_null:bool=False):
        self.value = value
        self.is_null = is_null
        self.result = bytearray()
        self.result.extend(self.value.encode('utf-8'))

    def len_variable(self):
        return True

    def space_use(self):
        return len(self.result)

    def get_bytes(self) -> bytearray:
        return self.result

    def __repr__(self):
        return f"str:{self.value}"


class IntValue(Value):
    def len_variable(self):
        return False
    def space_use(self):
        return 4
    def get_bytes(self) -> bytearray:
        result = bytearray(4)
        struct.pack_into('<i',result,0, self.value)
        return result
    def __init__(self,value:int,is_null:bool=False):
        self.value = value
        self.is_null = is_null

    def __repr__(self):
        return f"int:{self.value}"

class Row:
    def __init__(self,values:List[Value]):
        self.values = values
        self.space_use = self._space_use()
    def _space_use(self):
        use = 0
        for value in self.values:
            use += value.space_use()
        return use
    def __repr__(self):
        return str(self.values)

def over_flow_row(v: bytearray):
    return Row([ByteArray(v)])

def generate_row(v:List[int|str])->Row:
    values: List[Value] = []
    for value in v:
        if type(value) == int:
            values.append(IntValue(value))
        elif type(value) == str:
            values.append(StrValue(value))
    return Row(values)

print(generate_row([1,2,3,4,"hello world"]))

