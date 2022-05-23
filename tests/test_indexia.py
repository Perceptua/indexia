# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:12:44 2022

@author: aphorikles
"""

from datetime import datetime as dt
from indexia import indexia
import random
import unittest as ut

class TestIndexia(ut.TestCase):        
    def setUp(self):
        self.test = f'test{random.randint(0, 9999)}'
        self.test_db = 'tests/test.db'
    
    def testIndexia(self):
        created = dt.now().strftime('%Y-%m-%d-%H-%m')
        
        with indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, self.test)
            library = ix.add_library(cnxn, self.test, scribe)
            card = ix.add_card(cnxn, library, created=created)
            logonym = ix.add_logonym(cnxn, self.test, library, card)
        
        (scribe_id, pseudonym) = scribe.loc[0]
        (library_id, libronym, scribe_id_library) = library.loc[0]
        (card_id, created_card, library_id_card) = card.loc[0]
        (logonym, card_id_logonym) = logonym.loc[0]
        
        self.assertEqual(pseudonym, self.test)
        self.assertEqual(scribe_id, scribe_id_library)
        self.assertEqual(libronym, self.test)
        self.assertEqual(library_id, library_id_card)
        self.assertEqual(created, created_card)
        self.assertEqual(card_id, card_id_logonym)
        self.assertEqual(logonym, self.test)


if __name__ == '__main__':
    ut.main()