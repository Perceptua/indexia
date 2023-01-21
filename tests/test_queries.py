# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 10:06:15 2022

@author: aphorikles
"""

from inquiry import opera
import unittest as ut



class TestOpera(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tablename = 'users'
        cls.values = [('user1'), ('user2'), ('user3')]
        cls.columns = {'uid': 'INT PRIMARY KEY',
                       'username': 'VARCHAR(28)'}
        
    def testCreate(self):
        statement = opera.create(self.tablename, self.columns)
        expected = 'CREATE TABLE IF NOT EXISTS users (uid INT PRIMARY KEY,username VARCHAR(28))'
        self.assertEqual(statement, expected)
        
    def testInsert(self):        
        statement = opera.insert(self.tablename, [(i, f'user{i}') for i in range(1,4)])
        expected = "INSERT INTO users VALUES ('1','user1'),('2','user2'),('3','user3')"
        self.assertEqual(statement, expected)
        
    def testSelect(self):
        statement = opera.select(self.tablename, ['uid'], 'WHERE uid > 1')
        expected = 'SELECT uid FROM users WHERE uid > 1'
        self.assertEqual(statement, expected)
    

    
if __name__ == '__main__':
    ut.main()