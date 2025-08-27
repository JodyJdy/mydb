import struct

import config
from store.log.logger import RotatingLogger
import pickle
import threading



class LogEntry:
    def __init__(self,container_id,page_id,method,args,kwargs):
        self.container_id = container_id
        self.page_id = page_id
        self.method = method
        self.args = args
        self.kwargs = kwargs


class BinLog:
    def __init__(self):
        self.lock = threading.Lock()
        self.logger = RotatingLogger("binlog",config.LOG_FILE_PER_SIZE,config.LOG_BUFFER_SIZE)

    def write_log_entry(self,log_entry:LogEntry):
        byte_content = pickle.dumps(log_entry)
        size = len(byte_content)
        with self.lock:
            offset = self.logger.write_offset()
            self.logger.write(struct.pack("<i",size))
            self.logger.write(byte_content)
        return offset
    def flush(self):
        self.logger.flush()
    def close(self):
        self.logger.close()

binlog = BinLog()