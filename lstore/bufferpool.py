from lstore.page import Page, PageRange
from lstore.config import *
class BufferPool:
    def __init__(self):
        self.base_page_range_dict = {}
        self.tail_page_range_dict = {}



    def clear_bufferpool(self):
        self.base_page_range_dict.clear()
        self.tail_page_range_dict.clear()

    def add_page(self, page_type, column_index, page, page_range_index):
        page_range_dict = self.base_page_range_dict if page_type == "base" else self.tail_page_range_dict
        if column_index not in page_range_dict:
            page_range_dict[column_index] = []
        if len(page_range_dict[column_index]) == 0 or not page_range_dict[column_index][-1].has_capacity():
            page_range_dict[column_index].append(PageRange(MAX_PAGE_RANGE, page_range_index))
        page_range_dict[column_index][-1].add_page(page)