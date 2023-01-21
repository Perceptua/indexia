# -*- coding: utf-8 -*-
"""
Created on Sun May  1 15:06:11 2022

@author: aphorikles
"""

import os
import time
import webbrowser
import xml.etree.ElementTree as et



class tabula:
    def __init__(self):
        self.columns = {
            'scribes': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'pseudonym': 'VARCHAR(28) UNIQUE NOT NULL',
            },
            'libraries': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'libronym': 'VARCHAR(56) UNIQUE NOT NULL',
                'scribe_id': 'INTEGER NOT NULL',
                'FOREIGN KEY (scribe_id)': self.references('scribes', 'id'),
            },
            'cards': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'created': 'DATETIME UNIQUE NOT NULL',
                'library_id' : 'INTEGER NOT NULL',
                'FOREIGN KEY (library_id)': self.references('libraries', 'id'),
            },
            'logonyms': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'logonym': 'VARCHAR(56) NOT NULL',
                'card_id': 'INTEGER NOT NULL',
                'FOREIGN KEY (card_id)': self.references('cards', 'id'),
            },
        }
    
    def references(self, tablename, on_column, on_delete='CASCADE', on_update='CASCADE'):
        references = f'REFERENCES {tablename}({on_column})'
        references = f'{references} ON DELETE {on_delete} ON UPDATE {on_update}'
        
        return references



class xml:    
    def render_image(corpus):
        root = et.Element('root')
        libraries = {}
        cards = {}
        logonyms = {}
        
        for i, row in corpus.iterrows():
            if row['library_id'] not in libraries.keys():
                e = et.SubElement(
                    root, 
                    'library', 
                    id=str(row['library_id']),
                    libronym=str(row['libronym'])
                )
                
                libraries[row['library_id']] = e
            
            if row['card_id'] not in cards.keys():
                e = et.SubElement(
                    libraries[row['library_id']], 
                    'card',
                    id=str(row['card_id']),
                    created=str(row['created'])
                )
                
                cards[row['card_id']] = e
            
            if row['logonym_id'] not in logonyms.keys():
                e = et.SubElement(
                    cards[row['card_id']], 
                    'logonym', 
                    id=str(row['logonym_id']),
                    logonym=str(row['logonym'])
                )
                
                logonyms[row['logonym_id']] = e
        
        image = et.ElementTree(root)
        
        return image
    
    def display_image(image):
        file_path = 'data/corpus.xml'
        file_path = os.path.abspath(file_path)
        image.write(file_path)
        webbrowser.open(f'file://{file_path}')
        time.sleep(3)
        os.remove(file_path)