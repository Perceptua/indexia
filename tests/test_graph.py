# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 12:47:57 2022

@author: aphorikles
"""

from datetime import datetime as dt, timedelta as td
from graph import logograph
from indexia import indexia
import pandas as pd
import itertools, os
import unittest as ut



class TestGraph(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test = 'test'
        cls.test_db = 'tests/test.db'
        cls.logonyms = pd.DataFrame()
        cls.choices = ['one', 'two', 'three']
        
        with indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, cls.test)
            library = ix.add_library(cnxn, cls.test, scribe)
            
            for i in range(3):
                created = (dt.now() + td(minutes=i)).strftime('%Y-%m-%d-%H-%M') 
                card = ix.add_card(cnxn, library, created=created)
                [ix.add_logonym(cnxn, cls.choices[x], card) for x in range(3)]
                cls.logonyms = pd.concat([cls.logonyms,
                                          ix.get_logonyms(cnxn, card)])
        
        cls.graph = logograph(cls.logonyms)
    
    def testGetEdgeList(self):
        choices = [1, 2, 3]
        edge_list = self.graph.get_edge_list()
        expected_edge_list = [list(itertools.combinations(choices, 2)) \
                              for i in range(3)]
        
        self.assertEqual(edge_list, expected_edge_list)
    
    @classmethod
    def tearDownClass(cls):
        os.remove(cls.test_db)



if __name__ == '__main__':
    ut.main()