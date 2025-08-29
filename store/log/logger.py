import os
import glob
import threading
import io
from pathlib import Path

from typing import Dict, IO
import config


class FileReader:
    def __init__(self, path: Path,num:int):
        self.path = path
        self.num = num
        self.file = open(path, "rb")

    def size(self):
        return self.path.stat().st_size

    def seek(self, offset: int, whence: int = 0):
        self.file.seek(offset, whence)
    def read(self, n: int = -1):
        return self.file.read(n)

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
        #当前在写的文件，由于是顺序写，这个一定是编号最大的文件
        self.current_num = self._find_latest_log_number()

        if self.current_num == -1:
            self.current_num = 0

        #最大日志文件数

        #创建当前写的文件
        self.current_file = None
        self._open_current_file()

        # 初始化时检查是否需要分割
        if self._current_file_size() >= self.max_size:
            self._rotate()

        # 用于缓冲需要读取的文件
        self.file_reader_cache:Dict = {}

        #文件结尾位置
        self.end_position = self.current_num * self.max_size +  self.current_file.tell()


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
        file_path = Path(config.LOG_FILE_PATH)/filename
        return FileReader(file_path, num)

    def get_read_log_file(self, num:int):
        if num in self.file_reader_cache:
            return self.file_reader_cache[num]
        self.file_reader_cache[num] = self._open_read_file(num)
        return self.file_reader_cache[num]

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

    def write(self, data):
        msg_size = len(data)

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

            #对于 end_position的修改是线程安全的
            self.end_position +=msg_size

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
        free_space = self.max_size - self._current_file_size()
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
        result = bytearray()
        cur_num = offset // self.max_size
        file_pos = offset % self.max_size
        cur_file:FileReader = self.get_read_log_file(cur_num)
        cur_file.seek(file_pos)

        need_read_size = size
        while need_read_size != 0:
            #当前文件的size - 文件pos是可以读取的位置
            readable_size = cur_file.size() - file_pos
            #可以一次读完
            if need_read_size <= readable_size:
                result.extend(cur_file.read(need_read_size))
                need_read_size = 0
            #需要多次读取
            else:
                result.extend(cur_file.read(readable_size))
                need_read_size -= readable_size
                cur_num += 1
                file_pos = 0
                if cur_num > self.current_num:
                    raise Exception("文件读取越界")
                cur_file = self.get_read_log_file(cur_num)
                cur_file.seek(file_pos)
        return result

# 使用示例
if __name__ == "__main__":

    logger = RotatingLogger("app_log", max_size=50, buffer_max_size=10)
    b = bytearray(199)
    # logger.write(b)
    # logger.flush()

    print(logger.read(0,199))


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