from lstore.config import *
import struct
import base64
import os
import json


class Page:

    def __init__(self, schema_encoding_index, num_col, column_index, page_index):
        self.num_records = 0
        self.data = bytearray(MAX_PAGE_SIZE)
        self.schema_encoding_index = schema_encoding_index
        self.num_col = num_col
        self.column_index = column_index
        self.page_index = page_index
        self.pin = 0
        self.dirty = True

    def has_capacity(self):
        """
        # Check if the page has capacity for more records
        # :return: bool     #True if the page has capacity for more records, False otherwise
        """
        return self.num_records < MAX_RECORD_PER_PAGE

    def write(self, value):
        """
        # Appends the record to the page
        # :param value: int     #The value to be added to the page
        # :return: list         #The location of the record in the page
        # return None if the column is None
        """
        # This always happens when we write column for the tail page
        self.pin += 1
        if value == None:
            value = NONE_VALUE
            self.pin -= 1
        if self.has_capacity:
            begin = self.num_records * 4
            self.data[begin: begin + 4] = struct.pack("i", int(value))
            self.num_records += 1
            self.dirty = True
            self.pin -= 1
        else:
            self.pin -= 1
            raise MemoryError("This Page is full")

    def modify_value(self, value, index):
        """
        # Modify the value at the given index
        # :param value: int     #The value to be modified
        # :param index: int     #The index of the value to be modified
        """
        self.pin += 1
        begin = index * 4
        old_value = self.get_value(index)
        self.data[begin: begin + 4] = struct.pack("i", int(value))
        self.dirty = True
        self.pin -= 1
        return old_value

    def get_value(self, index):
        """
        # Get the value at the given index
        # :param index: int     #The index of the value to be retrieved
        # :return: int          #The value at the given index
        """
        self.pin += 1
        begin = index * 4
        value = struct.unpack("i", self.data[begin: begin + 4])[0]
        if (self.column_index == self.schema_encoding_index):
            value = str(value)
            value = value.rjust(self.num_col - 4, '0')
        if (value == NONE_VALUE):
            value = None
        self.pin -= 1
        return value

    def serialize(self):
        # Convert the binary data to a base64 string for JSON compatibility
        data_base64 = base64.b64encode(self.data).decode('utf-8')
        # Create a dictionary of the object's data
        obj_dict = {
            'num_records': self.num_records,
            'data': data_base64,
            'schema_encoding_index': self.schema_encoding_index,
            'num_col': self.num_col,
            'column_index': self.column_index,
            'page_index': self.page_index,
            'pin': self.pin,
            'dirty': self.dirty
        }
        # Serialize the dictionary to a JSON string
        return json.dumps(obj_dict)

    def deserialize(self, serialized_str):
        # Deserialize the JSON string back to a dictionary
        obj_dict = json.loads(serialized_str)
        # Decode the base64 string back to binary data
        self.data = bytearray(base64.b64decode(obj_dict['data']))

        self.schema_encoding_index = obj_dict['schema_encoding_index']
        self.num_col = obj_dict['num_col']
        self.column_index = obj_dict['column_index']
        self.page_index = obj_dict['page_index']
        self.pin = obj_dict['pin']
        self.dirty = obj_dict['dirty']
        # if (self.column_index == 5):
        #     for i in range(1024):
        #         print(struct.unpack("i", self.data[i * 4: i * 4 + 4])[0])



class PageRange:
    def __init__(self, max_capacity, page_range_index):
        # The maximum number of pages that can be stored in this page range
        self.max_capacity = max_capacity

        # For example, [Page1, Page2, Page3, ..., PageMaxCapacity]
        self.pages = []
        self.page_range_index = page_range_index

    def has_capacity(self):
        """
        # Check if the page range has capacity for more pages
        # :return: bool     #True if the page range has capacity for more pages, False otherwise
        """
        return len(self.pages) < self.max_capacity

    def add_page(self, page: Page):
        """
        # Add a page to the page range
        """
        if self.has_capacity():
            self.pages.append(page)
        else:
            raise MemoryError("This PageRange is full")

    def get_latest_page(self) -> Page:
        """
        # Get the latest page in the page range
        # :return: Page     #The latest page in the page range
        """
        return self.pages[-1]

    def get_page(self, index) -> Page:
        """
        # Get the page at the given index
        # :param index: int     #The index of the page to be retrieved
        # :return: Page         #The page at the given index
        """
        return self.pages[index]
