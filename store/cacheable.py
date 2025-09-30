
"""
页面类型
"""
OVER_FLOW_PAGE = 1
NORMAL_PAGE = 0
MANAGEMENT_PAGE = 2

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
        self.page_type = 0
    def sync(self):
        pass

    def lsn_offset(self)->int:
        """
        返回 lsn 字段 在page中的偏移
        :return:
        """
        pass

    def set_lsn(self, lsn):
        self.lsn = lsn

    def set_container(self, container):
        self.container = container
        self.container_id = self.container.container_id

    def write_page_cache(self):
        """
        写入文件的 page cache 但是不调用  file.flush()
        :return:
        """
        self.container.write_single_page(self)

    def flush(self):
        """
        调用文件的 file.flush()方法，为了单个页执行 flush成本太高
        几乎不会使用
        :return:
        """
        self.container.flush_single_page(self)

    def init(self):
        pass
