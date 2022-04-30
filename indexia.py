# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:22:34 2022

@author: aherkel
"""

from queries import querystore
import sqlite3
import pandas as pd



class indexia:
    def __init__(self, username, user_db='onomata.db'):
        self.username = username
        self.user_db = user_db
        self.cnxns = {}
    
    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        cnxns = self.cnxns.copy().keys()
        
        for cnxn in cnxns:
            self.close_cnxn(cnxn)
            
    def close_cnxn(self, cnxn):
        cnxn = self.cnxns.pop(cnxn)
        cnxn.close()
    
    def get_user(self, retry=True):
        user_cnxn = sqlite3.connect(self.user_db)
        self.cnxns[self.user_db] = user_cnxn
        
        try:
            user_query = f"WHERE username = '{self.username}'"
            user_query = querystore.select('users', ['*'], user_query)
            result = pd.read_sql(user_query, user_cnxn)
            
            return result
        
        except Exception as err:
            self.create_user_table(user_cnxn)
            
            if retry:
                return self.get_user(retry=False)
            else:
                raise err
    
    def create_user_table(self, user_cnxn):
        cols = {'uid': 'INT PRIMARY KEY',
                'username': 'VARCHAR(28) NOT NULL'}
            
        create_users = querystore.create('users', cols)
        insert_user = querystore.insert('users',
                                        [(1, self.username)])
        
        user_cnxn.execute(create_users)
        user_cnxn.execute(insert_user)
        user_cnxn.commit()
            
if __name__ == '__main__':
    with indexia('aphorikles') as ix:
        result = ix.get_user()
