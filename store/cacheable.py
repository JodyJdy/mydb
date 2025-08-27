class CacheablePage:
    """
    缓存页的统一父类
    """
    def __init__(self, page_num: int, page_data: bytearray):
        self.page_num = page_num
        self.page_data = page_data
        self.dirty = False