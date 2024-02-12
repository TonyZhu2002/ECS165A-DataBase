from lstore.db import Database
from lstore.query import Query
import unittest

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

class TestQuery(unittest.TestCase):
    
    def test_insert(self):
        for i in range(10):
            keys.append(i)
            self.assertTrue(query.insert(i, i+100, i+200, i+300, i+400))
            self.assertFalse(query.insert(i, i, i, i, i))
            

if __name__ == '__main__':
    unittest.main()