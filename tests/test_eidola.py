from indexia.eidola import Maker
from indexia.indexia import Indexia
import os
import pandas as pd
import unittest as ut


class TestEidola(ut.TestCase):
    def setUp(self):
        self.test_db = 'tests/data/test_eidola.db'
        self.species_per_genus = 3
        self.num_beings = 10
        self.trait = 'name'
        self.genus = 'creators'
        
        self.maker = Maker(
            self.test_db, 
            self.species_per_genus, 
            self.num_beings, 
            self.trait
        )
        
    def testMakeCreators(self):
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            creators = self.maker.make_creators(ix, cnxn, self.genus)
            exp_columns = ['id', self.trait]
            exp_expr = f'{self.genus}_0'
            self.assertEqual(list(creators.columns), exp_columns)
            self.assertEqual(creators.shape[0], self.num_beings)
            self.assertIn(exp_expr, list(creators[self.trait]))
        
    def testMakeCreatures(self):
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            self.maker.make_creators(ix, cnxn, self.genus)
            species = 'creatures'
            
            creatures = self.maker.make_creatures(
                ix, cnxn, self.genus, species
            )
            
            exp_columns = ['id', self.trait, f'{self.genus}_id']
            exp_expr = f'{species}_0'
            exp_fk = 1
            self.assertEqual(list(creatures.columns), exp_columns)
            self.assertEqual(creatures.shape[0], self.num_beings)
            self.assertIn(exp_expr, list(creatures[self.trait]))
            self.assertIn(exp_fk, list(creatures[f'{self.genus}_id']))
            
    
    def testMakeSpecies(self):
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            self.maker.make_creators(ix, cnxn, self.genus)
            species_prefix = 'creatures'
            
            species = self.maker.make_species(
                ix, cnxn, self.genus, species_prefix
            )
            
            self.assertEqual(len(species), self.species_per_genus)
            self.assertIsInstance(species[0], pd.DataFrame)
    
    def testMake(self):
        fathers, sons, grandsons, great_grandsons = self.maker.make()
        self.assertEqual(len(fathers), 1)
        self.assertEqual(len(sons), self.species_per_genus)
        self.assertEqual(len(grandsons), self.species_per_genus**2)
        self.assertEqual(len(great_grandsons), self.species_per_genus**3)
        
    def testGet(self):
        fathers, sons, grandsons, great_grandsons = self.maker.get()
        self.assertEqual(len(fathers), 1)
        self.assertEqual(len(sons), self.species_per_genus)
        self.assertEqual(len(grandsons), self.species_per_genus**2)
        self.assertEqual(len(great_grandsons), self.species_per_genus**3)
        
    def tearDown(self):
        try:
            os.remove(self.test_db)
        except:
            pass


if __name__ == '__main__':
    ut.main()
