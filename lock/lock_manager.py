import threading

class LockEntry:
    def __init__(self):
        self.read_count = 0
        self.write_holder = None
        self.cond = threading.Condition()

    def is_compatible(self, lock_type, thread_id):
        if lock_type == 'read':
            return self.write_holder is None
        else:  # write
            return self.read_count == 0 and self.write_holder is None

class LockManager:
    def __init__(self):
        self.locks = {}
        self.global_lock = threading.Lock()

    def acquire(self, resource, lock_type, thread_id):
        with self.global_lock:
            if resource not in self.locks:
                self.locks[resource] = LockEntry()
            entry = self.locks[resource]

        while True:
            with entry.cond:
                if entry.is_compatible(lock_type, thread_id):
                    if lock_type == 'read':
                        entry.read_count += 1
                    else:
                        entry.write_holder = thread_id
                    return True
                # 等待锁释放
                entry.cond.wait()

    def release(self, resource, lock_type, thread_id):
        with self.global_lock:
            if resource not in self.locks:
                return
            entry = self.locks[resource]

        with entry.cond:
            if lock_type == 'read':
                if entry.read_count <= 0:
                    return  # 错误：释放未持有的读锁
                entry.read_count -= 1
            else:
                if entry.write_holder != thread_id:
                    return  # 错误：释放未持有的写锁
                entry.write_holder = None
            # 通知所有等待线程
            entry.cond.notify_all()

# 示例用法
if __name__ == "__main__":
    lm = LockManager()

    # 线程1获取表锁写锁
    lm.acquire(('orders',), 'write', 'thread1')
    print("Thread1 acquired table write lock")

    # 线程2尝试获取同一表的读锁（会被阻塞）
    def thread2_task():
        lm.acquire(('orders',), 'read', 'thread2')
        print("Thread2 acquired table read lock")
        lm.release(('orders',), 'read', 'thread2')

    t2 = threading.Thread(target=thread2_task)
    t2.start()

    # 等待1秒后释放锁
    threading.Timer(1.0, lambda: lm.release(('orders',), 'write', 'thread1')).start()
    t2.join()