from indexia.inquiry import Inquiry, Tabula
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


class TestTabula(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.creator = 'scribes'
        cls.creature = 'libraries'
        cls.creator_attr = 'pseudonym'
        cls.creature_attr = 'libronym'
        
    def testGetCreatorTable(self):
        tablename, cols = Tabula.get_creator_table(
            self.creator, self.creator_attr
        )
        
        self.assertEqual(self.creator, tablename)
        self.assertEqual({'id', self.creator_attr}, set(cols.keys()))
        
    def testGetCreatureTable(self):
        tablename, cols = Tabula.get_creature_table(
            self.creator, self.creature, self.creature_attr
        )
        
        self.assertEqual(self.creature, tablename)
        
        self.assertEqual({
            'id', 
            self.creature_attr, 
            f'{self.creator}_id', 
            f'FOREIGN KEY ({self.creator}_id)'
        }, set(cols.keys()))
    
    def testReferences(self):
        references = Tabula.references(
            self.creator, 
            self.creator_attr
        )
        
        expected = ' '.join([
            f'REFERENCES {self.creator}({self.creator_attr})',
            'ON DELETE CASCADE ON UPDATE CASCADE'
        ])
        
        self.assertEqual(references, expected)


if __name__ == '__main__':
    ut.main()