import os
import glob
import threading
import io
from pathlib import Path

from typing import Dict, IO

import config


class RotatingLogger:
    """
    当前类，只进行 二进制的写入/读取相关，不考虑里面的内容是什么
    """
    def __init__(self, log_prefix, max_size=config.LOG_FILE_PER_SIZE, buffer_max_size=config.LOG_BUFFER_SIZE):
        """
        :param log_prefix:
        :param max_size: 日志文件大小
        :param buffer_max_size: 缓冲区大小
        """
        config.create_log_directory_if_need()
        self.log_prefix = log_prefix
        self.max_size = max_size
        self.buffer_size_limit = buffer_max_size
        self.buffer = io.BytesIO()
        self.buffer_bytes = 0
        self.lock = threading.Lock()
        self.current_num = self._find_latest_log_number()


        if self.current_num == -1:
            self.current_num = 0

        #创建当前写的文件
        self.current_file = None
        self._open_current_file()

        # 初始化时检查是否需要分割
        if self._current_file_size() >= self.max_size:
            self._rotate()

        # 用于缓冲需要读取的文件
        self.read_file_cache:Dict = {}


    def _find_latest_log_number(self):
        pattern = f"{config.LOG_FILE_PATH}/{self.log_prefix}_*.log"
        files = glob.glob(pattern)
        max_num = -1

        for file in files:
            base = os.path.basename(file)
            name_part = os.path.splitext(base)[0]
            parts = name_part.split('_')

            if len(parts) < 2:
                continue

            num_str = parts[-1]
            if num_str.isdigit():
                num = int(num_str)
                if num > max_num:
                    max_num = num

        return max_num if max_num != -1 else -1

    def _get_filename(self, num):
        return f"{self.log_prefix}_{num}.log"

    def _open_read_file(self,num:int):
        filename = self._get_filename(num)
        return open(Path(config.LOG_FILE_PATH)/filename, 'rb')  # 二进制追加模式

    def get_read_log_file(self,offset:int):
        num = offset // self.max_size
        if num in self.read_file_cache:
            return self.read_file_cache[num]
        self.read_file_cache[num] = self._open_read_file(num)
        return self.read_file_cache[num]

    def _open_current_file(self):
        filename = self._get_filename(self.current_num)
        self.current_file = open(Path(config.LOG_FILE_PATH)/filename, 'ab')  # 二进制追加模式

    def _current_file_size(self):
        return self.current_file.tell()

    def _rotate(self):
        if self.current_file and not self.current_file.closed:
            self.current_file.close()
        self.current_num += 1
        self._open_current_file()

    def write_offset(self):
        """本次写入的偏移位置"""
        return self.current_num * self.max_size + self.buffer_bytes

    def write(self, message):
        data = message.encode('utf-8')
        msg_size = len(data)

        offset = self.write_offset()

        with self.lock:
            wrote_size = 0
            while wrote_size < msg_size:
                buffer_free = self.buffer_size_limit - self.buffer_bytes
                #本次写入数据量
                step_write = min(buffer_free, msg_size - wrote_size)
                self.buffer.write(data[wrote_size:wrote_size+step_write])
                self.buffer_bytes += step_write
                if self.buffer_bytes >= self.buffer_size_limit:
                    self._flush_buffer()
                wrote_size+=step_write

        return offset

    def flush(self):
        self._flush_buffer()


    def _flush_buffer(self):
        data = self.buffer.getvalue()
        self.buffer.seek(0)
        self.buffer.truncate(0)
        self.buffer_bytes = 0

        # 检查文件大小并分割
        current_file_size = self._current_file_size()
        if current_file_size >= self.max_size:
            #切换新文件
            self._rotate()

        #计算当前文件是否满足
        free_space = self.max_size - current_file_size
        if free_space >= len(data):
            self.current_file.write(data)
            self.current_file.flush()
        else:
            #分两次写
            self.current_file.write(data[0:free_space])
            self.current_file.flush()
            #切换新文件
            self._rotate()
            self.current_file.write(data[free_space:])
            self.current_file.flush()

    def close(self):
        with self.lock:
            self._flush_buffer()
        if self.current_file and not self.current_file.closed:
            self.current_file.close()

    def read(self,offset,size):
        file:IO = self.get_read_log_file(offset)
        file.seek(offset % self.buffer_size_limit)
        print(file.read(size))

# 使用示例
if __name__ == "__main__":

    logger = RotatingLogger("app_log", max_size=1024, buffer_max_size=100)

    # 模拟多线程写入
    # def worker():
    #     for i in range(200):
    #         logger.write(f"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaLog entry {i}\n")
    #
    # worker()
    # # threads = []
    # # for _ in range(4):
    # #     t = threading.Thread(target=worker)
    # #     threads.append(t)
    # #     t.start()
    # #
    # # for t in threads:
    # #     t.join()
    #
    # logger.close()
    print(logger.read(1024,100))