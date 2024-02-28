class BufferPool:
    def __init__(self):
        self.base_page_range_dict = {}
        self.tail_page_range_dict = {}