from abc import abstractmethod

from typing import List


class Value:
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

class Row:
    def __init__(self,values:List[Value|bytearray]):
        self.values = values
        self.space_use = self._space_use()
    def values(self)->List[Value|bytearray]:
        pass
    def _space_use(self):
        use = 0
        for value in self.values:
            if isinstance(value, bytearray):
                use+=len(value)
            else:
                use += value.space_use()
        return use

    @staticmethod
    def single_value_row(v:Value|bytearray):
        return Row([v])


