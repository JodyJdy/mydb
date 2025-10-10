import os
import threading
import io
from typing import Dict, IO
import config

class RedoLogManager:

    def __init__(self, log_dir = config.REDO_LOG_FILE_PATH, file_prefix="redo", file_size=config.REDO_LOG_FILE_PER_SIZE,buffer_max_size=config.REDO_BUFFER_SIZE):  # 默认2个100MB文件
        config.create_redo_log_directory_if_need()
        self.log_dir = log_dir
        self.file_prefix = file_prefix
        self.file_size = file_size
        self.write_pos = 0  # 当前文件内的写入位置

        #check_pos <= read_pos <= write_pos
        self.read_pos = 0
        self.check_pos = 0  # 已持久化的位置
        #全局写入位置，持久化位置
        self.global_write_pos = 0
        self.global_check_pos = 0
        self._init_log_files()
        self.write_lock = threading.Lock()
        self.read_lock = threading.Lock()
        self.write_flush_lock = threading.Lock()
        self.read_flush_lock = threading.Lock()
        self.buffer_size_limit = buffer_max_size
        self.write_buffer = io.BytesIO()
        #打开要操作的文件
        self.write_redo_file = self._open_write_redo_file()
        self.read_redo_file = self._open_read_redo_file()

        self.read_buffer = io.BytesIO()
        #读取buffer大小
        self.read_buffer_bytes = 0
        # 读buffer中已经读取的大小
        self.has_read_bytes = 0
        self.read_buffer_data_len = 0



    def _write_buffer_data_len(self):
        """
        write buffer只写不读，可以直接获取内容长度
        :return:
        """
        return self.write_buffer.tell()

    def _read_buffer_data_len(self):
        return self.read_buffer_data_len


    def _open_write_redo_file(self):
        f = open(self._get_file_path(), 'r+b')
        f.seek(self.write_pos)
        return f

    def _open_read_redo_file(self):
        f = open(self._get_file_path(), 'r+b')
        f.seek(self.check_pos)
        return f

    def free_space(self):
        """
        返回空闲空间
        :return:
        """
        temp_write_pos = (self.write_pos + self._write_buffer_data_len()) % self.file_size

        if self.check_pos == temp_write_pos:
            return self.file_size
        if self.check_pos < temp_write_pos:
            return (self.file_size - temp_write_pos) + self.check_pos
        else:
            return self.check_pos - temp_write_pos

    def used_space(self):
        """
        返回已经使用的空间
        :return:
        """
        temp_write_pos = (self.write_pos + self._write_buffer_data_len()) % self.file_size
        if self.check_pos == temp_write_pos:
            return 0
        if self.check_pos < temp_write_pos:
            return temp_write_pos - self.check_pos
        else:
            return (self.file_size - self.check_pos) + temp_write_pos


    def _init_log_files(self):
        """初始化日志文件"""
        path = os.path.join(self.log_dir, f"{self.file_prefix}.log")
        if not os.path.exists(path):
            with open(path, 'wb') as f:
                f.write(b'\x00' * self.file_size)  # 预分配空间

    def _get_file_path(self):
        """获取指定索引的日志文件路径"""
        return os.path.join(self.log_dir, f"{self.file_prefix}.log")


    def _write_redo_with_flush(self,data):
        self.write_redo_file.write(data)
        self.write_redo_file.flush()
        os.fsync(self.write_redo_file.fileno())


    def _flush_write_buffer(self):
        with self.write_flush_lock:
            data = self.write_buffer.getvalue()
            self.write_buffer.seek(0)
            self.write_buffer.truncate(0)

            if self.write_pos < self.check_pos or self.file_size - self.write_pos >=len(data):
                    self._write_redo_with_flush(data)
            else:
                    step_size = self.file_size - self.write_pos
                    self.write_redo_file.write(data[0:step_size])
                    #切换到文件开始位置
                    self.write_redo_file.seek(0)
                    self.write_redo_file.write(data[step_size:])
                    self.write_redo_file.flush()
                    os.fsync(self.write_redo_file.fileno())
            #调整偏移量,只有 flush的才是可以读取的，一部分数据总是存放在 buffer中，需要考虑
            self.write_pos = (self.write_pos + len(data)) % self.file_size
            self.global_write_pos = self.global_write_pos + len(data)

    def write_entry(self, data):
        """
        写入后调整写的偏移量
        """
        """写入日志条目"""
        data_len = len(data)

        with self.write_lock:
            #空间不够
            if data_len > self.free_space() + 1:
                return -1
            wrote_size = 0
            while wrote_size < data_len:
                buffer_free = self.buffer_size_limit - self._write_buffer_data_len()
                #本次写入数据量
                step_write = min(buffer_free, data_len - wrote_size)
                self.write_buffer.write(data[wrote_size:wrote_size + step_write])

                if self._write_buffer_data_len() >= self.buffer_size_limit:
                    self._flush_write_buffer()
                wrote_size+=step_write

    def _flush_read_buffer(self):
        with self.read_flush_lock:
            #清空当前的读取buffer
            self.read_buffer.seek(0)
            self.read_buffer.truncate(0)


            #一次读取一个完整的buffer
            need_read_size = self.buffer_size_limit

            #从文件读取内容
            # 1.尝试从文件读取并填充整个buffer
            # 2. 如果从文件中不能读取到整个buffer，flush当前buffer的内容到文件中，再次进行读取，有多少读取多少


            has_flush = False
            while need_read_size != 0:
                temp_write_pos = self.write_pos

                if temp_write_pos > self.read_pos:
                    could_read_size = temp_write_pos - self.read_pos
                    #一次读取完
                    if could_read_size > need_read_size:
                        self.read_buffer.write(self.read_redo_file.read(need_read_size))
                        self.read_pos=(self.read_pos + need_read_size) % self.file_size
                        break
                    else:
                        self.read_buffer.write(self.read_redo_file.read(could_read_size))
                        self.read_pos =(self.read_pos + could_read_size) % self.file_size
                        need_read_size-=could_read_size

                elif  temp_write_pos < self.read_pos:
                    could_read_size = self.file_size - self.read_pos + temp_write_pos
                    if could_read_size > need_read_size:
                        if self.file_size - self.read_pos >= need_read_size:
                            self.read_buffer.write(self.read_redo_file.read(need_read_size))
                            self.read_pos = (self.read_pos + need_read_size) % self.file_size
                        else:
                            self.read_buffer.write(self.read_redo_file.read(could_read_size))
                            self.read_pos = (self.read_pos + could_read_size) % self.file_size
                        break
                    else:
                        self.read_buffer.write(self.read_redo_file.read(self.file_size - self.read_pos))
                        self.read_redo_file.seek(0)
                        self.read_buffer.write(self.read_redo_file.read(temp_write_pos))
                        self.read_pos = temp_write_pos

                if has_flush:
                    break
                #尝试flush一下buffer
                self._flush_write_buffer()
                has_flush = True

            #写完后，seek 0用于后续的读取
            self.has_read_bytes = 0
            #记录当前buffer数据长度
            self.read_buffer_data_len = self.read_buffer.tell()
            self.read_buffer.seek(0)


    def read(self,n:int = -1):
        """
        不调整读取的偏移量
        """
        with self.read_lock:
            result = bytearray()
            #已经读取的size
            read_size = 0
            could_read = self._read_buffer_data_len() -  self.has_read_bytes
            while read_size < n:
                #可以读取的长度
                if could_read != 0:
                    #可以读取完
                    if could_read > n - read_size:
                        result.extend(self.read_buffer.read(n-read_size))
                        self.has_read_bytes += n - read_size
                        break
                    #部分读取
                    else:
                        result.extend(self.read_buffer.read(could_read))
                        read_size += could_read
                        self.has_read_bytes += could_read
                #无可以读取的数据，或者 buffer为空，尝试刷新一下buffer的内容
                self._flush_read_buffer()
                could_read = self._read_buffer_data_len() -  self.has_read_bytes
                if could_read == 0:
                    break
            return result


    def advance_checkpoint(self, advance_len):
        with self.write_lock:
            self.check_pos = (self.check_pos + advance_len) % self.file_size
            self.global_check_pos += advance_len

# 使用示例
if __name__ == "__main__":
    log_manager = RedoLogManager(file_size=200,buffer_max_size=10)
    for i in range(10):
        content = b"helloworld12" *15
        log_manager.write_entry(content)
        log_manager._flush_write_buffer()
        print(log_manager.read(len(content)))
        log_manager.advance_checkpoint(len(content))
        print(log_manager.write_pos)
        print(log_manager.read_pos)
        print(log_manager.check_pos)
    # buffer = io.BytesIO()
    # buffer.write(b'hello')
    # v = buffer.getvalue()
    # print(v)
    # buffer.write(b"workd")
    # buffer.seek(0 )
    # print(buffer.read())
