import struct

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < 1024

    def write(self, value):
        if self.has_capacity:
            begin = self.num_records * 4   
            self.data[begin : begin + 4] = struct.pack("i", value)   
            self.num_records += 1
        else: 
            raise MemoryError("Page is full")     

