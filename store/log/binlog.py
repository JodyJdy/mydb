import struct

import config
from store.log.logger import RotatingLogger
import pickle
import threading

from collections.abc import Iterable,Sized

class LogEntry:
    def __init__(self):
        """
        在日志文件中的偏移量
        """
        self.entry_pos = 0
    def set_entry_pos(self, entry_pos):
        self.entry_pos = entry_pos
    def get_entry_pos(self):
        return self.entry_pos
    def serialize(self)->bytearray:
        pass
class PhysicalPageLogEntry(LogEntry):
    """
    物理页修改操作的 日志条目
    """
    def __init__(self,container_id,page_id,offset,data:bytearray|int|bytes):
        super().__init__()
        """
        :param container_id: container id
        :param page_id: 页面id
        :param offset: 修改数据的偏移量
        :param data:  填充的数据
        """
        self.container_id = container_id
        self.page_id = page_id
        self.offset = offset
        self.data = data


    def serialize(self):
        result = bytearray()
        result.extend(struct.pack("<iii",self.container_id,self.page_id,self.offset))
        if isinstance(self.data,Sized):
            result.extend(self.data)
        else:
            result.append(self.data)
        return result

    @staticmethod
    def deserialize(entry_bytes):
        container_id,page_id,offset = struct.unpack_from("<iii", entry_bytes, 0)
        data= entry_bytes[12:]
        return PhysicalPageLogEntry(container_id,page_id,offset,data)


class BinLog:
    def __init__(self):
        self.lock = threading.Lock()
        self.logger = RotatingLogger("binlog",config.LOG_FILE_PER_SIZE,config.LOG_BUFFER_SIZE)

    def write_log_entry(self,log_entry:LogEntry):
        byte_content = log_entry.serialize()
        size = len(byte_content)
        with self.lock:
            offset = self.logger.write_offset()
            self.logger.write(struct.pack("<i",size))
            self.logger.write(byte_content)
        return offset

    def read_log_entry(self,offset):
        end_pos = self.log_end_pos()
        while offset < end_pos:
            size = struct.unpack_from("<i",self.logger.read(offset,4),0)[0]
            entry = PhysicalPageLogEntry.deserialize(self.logger.read(offset + 4,size))
            entry.set_entry_pos(offset)
            yield entry
            offset+=4
            offset+=size

    def log_end_pos(self):
        return self.logger.write_offset() + self.logger.current_num * config.LOG_FILE_PER_SIZE

    def flush(self):
        self.logger.flush()
    def close(self):
        self.logger.close()

binlog = BinLog()