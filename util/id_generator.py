import time
import socket
import os
import struct
import hashlib

hostname = socket.gethostname()  # 获取主机名
md5 = hashlib.md5(hostname.encode()).digest()
machine_bytes = md5[:3]  # 取前3字节
pid = os.getpid()  # 获取进程ID
process_bytes = struct.pack(">H", pid & 0xFFFF)  # 取低16位，大端序2字节

def generate_object_id():
    # 1. 时间戳部分 (4字节)
    timestamp = int(time.time())  # 当前时间戳（秒）
    time_bytes = struct.pack(">I", timestamp)  # 大端序4字节
    random_bytes = os.urandom(3)  # 生成3字节随机数
    # 组合所有部分
    object_id = time_bytes + machine_bytes + process_bytes + random_bytes
    # 转换为十六进制字符串
    return ''.join(f"{b:02x}" for b in object_id)

# 测试生成
if __name__ == "__main__":
    for i in range(10):
        oid = generate_object_id()
        print(f"生成的ObjectId: {oid}")
        print(f"长度验证: {len(oid)} 字符 (应为24)")