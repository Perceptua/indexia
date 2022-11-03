# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:12:44 2022

@author: aphorikles
"""

from datetime import datetime as dt
from indexia import indexia
import os
import unittest as ut



class TestIndexia(ut.TestCase):
    @classmethod        
    def setUpClass(cls):
        cls.test_db = 'tests/test.db'
        cls.created = dt.now().strftime('%Y-%m-%d-%H-%M')
        
        with indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            cls.scribe = ix.add_scribe(cnxn, 'test')
            cls.library = ix.add_library(cnxn, 'test', cls.scribe)
            cls.card = ix.add_card(cnxn, cls.library, created=cls.created)
            cls.logonym = ix.add_logonym(cnxn, 'test', cls.card)
        
        (cls.scribe_id, cls.pseudonym) = cls.scribe.loc[0]
        (cls.library_id, cls.libronym, cls.scribe_id_library) = cls.library.loc[0]
        (cls.card_id, cls.created_card, cls.library_id_card) = cls.card.loc[0]
        (cls.logonym_id, cls.logonym, cls.card_id_logonym) = cls.logonym.loc[0]
    
    def testAdd(self):        
        self.assertEqual(self.pseudonym, 'test')
        self.assertEqual(self.scribe_id, self.scribe_id_library)
        self.assertEqual(self.libronym, 'test')
        self.assertEqual(self.library_id, self.library_id_card)
        self.assertEqual(self.created, self.created_card)
        self.assertEqual(self.card_id, self.card_id_logonym)
        self.assertEqual(self.logonym, 'test')
        
    def testUpdateAndDelete(self):
        rows_updated = 0
        rows_deleted = 0
        
        with indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            
            rows_updated += ix.update(cnxn, 'scribes', 
                                     ['pseudonym'], ['test1'], 
                                     ['id'], [1])
            
            self.assertEqual(rows_updated, 1)
            
            # expect 1
            rows_deleted += ix.delete(cnxn, 'logonyms', self.logonym_id)
            rows_deleted += ix.delete(cnxn, 'cards', self.card_id)
            rows_deleted += ix.delete(cnxn, 'scribes', self.scribe_id)
            self.assertEqual(rows_deleted, 3)
            
            # expect 0 due to ON_DELETE=CASCADE constraint
            rows_deleted += ix.delete(cnxn, 'libraries', self.scribe_id)
            self.assertEqual(rows_deleted, 3)        
    
    @classmethod
    def tearDownClass(cls):
        os.remove(cls.test_db)



if __name__ == '__main__':
    ut.main()