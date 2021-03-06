# -*- coding: utf-8 -*-
"""
Created on Sun May  1 15:06:11 2022

@author: aphorikles
"""

class tabula:
    def __init__(self):
        self.columns = {
            'scribes': {
                'scribe_id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'pseudonym': 'VARCHAR(28) UNIQUE NOT NULL',
            },
            'libraries': {
                'library_id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'libronym': 'VARCHAR(56) UNIQUE NOT NULL',
                'scribe_id': 'INTEGER NOT NULL',
                'FOREIGN KEY (scribe_id)': self.references('scribes', 'scribe_id'),
            },
            'cards': {
                'card_id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'created': 'DATETIME UNIQUE NOT NULL',
                'library_id' : 'INTEGER NOT NULL',
                'FOREIGN KEY (library_id)': self.references('libraries', 'library_id'),
            },
            'logonyms': {
                'logonym': 'VARCHAR(56) NOT NULL',
                'card_id': 'INTEGER NOT NULL',
                'FOREIGN KEY (card_id)': self.references('{libronym}', 'card_id'),
            },
        }
    
    def references(self, tablename, on_column, on_delete='CASCADE', on_update='CASCADE'):
        references = f'REFERENCES {tablename}({on_column})'
        references = f'{references} ON DELETE {on_delete} ON UPDATE {on_update}'
        
        return references