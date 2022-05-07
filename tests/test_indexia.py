# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:12:44 2022

@author: aphorikles
"""

from indexia import indexia
import unittest as ut

class TestIndexia(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pseudonym = 'aphorikles'
    
    def testGetOrCreate(self):
        with indexia(self.pseudonym) as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.get_or_create(cnxn, 'scribes', ['pseudonym'], [self.pseudonym])
            scribe = scribe.loc[0]
        
        self.assertEqual(scribe['pseudonym'], self.pseudonym)
        self.assertEqual(scribe['scribe_id'], 1)
        
    def testAddIndex(self):
        indexonym = 'test'
        with indexia(self.pseudonym) as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.get_or_create(cnxn, 'scribes', ['pseudonym'], [self.pseudonym])
            scribe_id = scribe.loc[0, 'scribe_id']
            index = ix.add_index(cnxn, indexonym, scribe_id).loc[0]
            
        self.assertEqual(index['index_id'], 1)
        self.assertEqual(index['indexonym'], indexonym)
        self.assertEqual(index['scribe_id'], 1)
        


if __name__ == '__main__':
    ut.main()