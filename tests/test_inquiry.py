from indexia.inquiry import Inquiry
import unittest as ut


class TestInquiry(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tablename = 'users'
        cls.values = [('user1'), ('user2'), ('user3')]
        
        cls.columns = {
            'uid': 'INT PRIMARY KEY',
            'username': 'VARCHAR(28)'
        }
        
    def testCreate(self):
        statement = Inquiry.create(self.tablename, self.columns)
        
        expected = ' '.join([
            'CREATE TABLE IF NOT EXISTS users',
            '(uid INT PRIMARY KEY,username VARCHAR(28))'
        ])
        
        self.assertEqual(statement, expected)
        
    def testInsert(self):        
        statement = Inquiry.insert(
            self.tablename, 
            [(i, f'user{i}') for i in range(1, 4)]
        )
        
        expected = ' '.join([
            'INSERT INTO users VALUES',
            "('1','user1'),('2','user2'),('3','user3')"
        ])
        
        self.assertEqual(statement, expected)
        
    def testSelect(self):
        statement = Inquiry.select(
            self.tablename, 
            ['uid'], 
            'WHERE uid > 1'
        )
        
        expected = 'SELECT uid FROM users WHERE uid > 1'
        self.assertEqual(statement, expected)
    
    def testDelete(self):
        statement = Inquiry.delete(self.tablename)
        expected = 'DELETE FROM users '
        self.assertEqual(statement, expected)
        
        statement = Inquiry.delete(
            self.tablename, 
            conditions="WHERE username = 'user1'"
        )
        
        expected = "DELETE FROM users WHERE username = 'user1'"
        self.assertEqual(statement, expected)
        
    def testUpdate(self):
        statement = Inquiry.update(
            self.tablename, 
            ['username'], 
            ['user4'],
            conditions="WHERE username = 'user1'"
        )
        
        expected = ' '.join([
            "UPDATE users SET username = 'user4'",
            "WHERE username = 'user1'"
        ])
        
        self.assertEqual(statement, expected)
    
    def testWhere(self):
        statement = Inquiry.where(
            ['username', 'username'], 
            ['user1', 'user2'],
            conjunction='OR'
        )
        
        expected = "WHERE username = 'user1' OR username = 'user2'"
        self.assertEqual(statement, expected)


if __name__ == '__main__':
    ut.main()