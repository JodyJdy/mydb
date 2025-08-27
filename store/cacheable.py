class CacheablePage:
    """
    缓存页的统一父类
    """
    def __init__(self, page_num: int, page_data: bytearray):
        self.page_num = page_num
        self.page_data = page_data
        self.container = None
        self.container_id = None
        self.dirty = False
        self.lsn = 0
    def sync(self):
        pass

    def set_container(self, container):
        self.container = container
        self.container_id = self.container.container_id

    def flush(self):
        self.container.flush_single_page(self)
