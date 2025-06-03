from store.values import Row


class BasePage:
    OVER_FLOW_PAGE = 1

    def __init__(self, page_num: int, page_data: bytearray):
        self.page_num = page_num
        self.page_data = page_data

    def sync(self):
        """
        状态同步到 page_data中
        :return:
        """
        pass

    def header_size(self) -> int:
        pass

    def insert_slot(self, row: Row, slot: int):
        pass

    def init_page(self):
        """新创建页面时，需要初始化"""
        pass