from lstore.db import Database
from lstore.query import Query
import unittest

db = Database()
key_index = 0
grades_table = db.create_table('Grades', 5, key_index)
query = Query(grades_table)
records = []

class TestQuery(unittest.TestCase):
    
    def test_insert(self):
        for i in range(10):
            records.append([i, i, i, i, i])
            records.append([i+100, i+100, i+200, i+300, i+400])
            self.assertTrue(query.insert(i, i, i, i, i))
            self.assertFalse(query.insert(i, i+100, i+200, i+300, i+400))
            self.assertTrue(query.insert(i+100, i+100, i+200, i+300, i+400))
            
        for record in records:
            key = record[key_index]
            for i in range(len(record)):
                address = grades_table.index.base_page_indices[i][key]
                self.assertEqual(query.get_page_value(address), record[i])

if __name__ == '__main__':
    unittest.main()