from lstore.db import Database
from lstore.query import Query
import unittest
from random import choice, randrange

db = Database()
key_index = 0
grades_table = db.create_table('Grades', 5, key_index)
query = Query(grades_table)
records = {}
keys = []

class TestQuery(unittest.TestCase):
    
    def test_insert(self):
        for i in range(10):
            records[i] = [i, i, i, i, i]
            keys.append(i)
            records[i+100] = [i+100, i+100, i+200, i+300, i+400]
            keys.append(i+100)
            self.assertTrue(query.insert(records[i]))
            self.assertFalse(query.insert(i, i+100, i+200, i+300, i+400))
            self.assertTrue(query.insert(records[i+100]))
            
        for key in keys:
            expected_record = records[key]
            for i in range(len(record)):
                address = grades_table.index.base_page_indices[i][key]
                self.assertEqual(query.get_page_value(address), record[i])
    
    def test_update(self):
        
        for i in range(len(records)):
            if (keys[i] + 200 not in keys):
                self.assertFalse(query.update(keys[i] + 200, *records[i]))

        for record in records:
            for i in range(len(record)-1):
                record[i+1] += 1
        
        for i in range(len(records)):
            self.assertTrue(query.update(keys[i], *records[i]))
        
        for record in records:
            key = record[key_index]
            tail_rid_address = grades_table.index.base_page_indices[grades_table.indirection_index][key]
            tail_rid = query.get_page_value(tail_rid_address)
            for i in range(len(record)):
                data = record[i]
                self.assertEqual(query.get_page_value(grades_table.index.tail_page_indices[i][tail_rid]), data)
        
    update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]
    
    for i in range(100):
        key = choice(keys)
        updated_list = choice(update_cols)
        for old_list in records:
            if old_list[key_index] == key:
                
            
            
            
            
        
if __name__ == '__main__':
    unittest.main()