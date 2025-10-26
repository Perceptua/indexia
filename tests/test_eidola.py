from indexia.eidola import Maker, Templates
from indexia.indexia import Indexia
from sqlite3 import Connection
import os
import pandas as pd
import unittest as ut


class TestEidola(ut.TestCase):
    def setUp(self) -> None:
        self.test_db: str = 'tests/data/test_eidola.db'
        self.species_per_genus: int = 3
        self.num_beings: int = 10
        self.trait: str = 'name'
        self.genus: str = 'creators'
        
        self.maker = Maker(
            self.test_db, 
            self.species_per_genus, 
            self.num_beings, 
            self.trait
        )
        
    def testMakeCreators(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            creators: pd.DataFrame = self.maker.make_creators(ix, cnxn, self.genus)
            exp_columns: list[str] = ['id', self.trait]
            exp_expr: str = f'{self.genus}_0'
            self.assertEqual(list(creators.columns), exp_columns)
            self.assertEqual(creators.shape[0], self.num_beings)
            self.assertIn(exp_expr, list(creators[self.trait]))
        
    def testMakeCreatures(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            self.maker.make_creators(ix, cnxn, self.genus)
            species: str = 'creatures'
            
            creatures: pd.DataFrame = self.maker.make_creatures(
                ix, cnxn, self.genus, species
            )
            
            exp_columns: list[str] = ['id', self.trait, f'{self.genus}_id']
            exp_expr: str = f'{species}_0'
            exp_fk: int = 1
            self.assertEqual(list(creatures.columns), exp_columns)
            self.assertEqual(creatures.shape[0], self.num_beings)
            self.assertIn(exp_expr, list(creatures[self.trait]))
            self.assertIn(exp_fk, list(creatures[f'{self.genus}_id']))
            
    
    def testMakeSpecies(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            self.maker.make_creators(ix, cnxn, self.genus)
            species_prefix: str = 'creatures'
            
            species: list[pd.DataFrame] = self.maker.make_species(
                ix, cnxn, self.genus, species_prefix
            )
            
            self.assertEqual(len(species), self.species_per_genus)
            self.assertIsInstance(species[0], pd.DataFrame)
    
    def testMake(self) -> None:
        fathers: list[pd.DataFrame]
        sons: list[pd.DataFrame]
        grandsons: list[pd.DataFrame]
        great_grandsons: list[pd.DataFrame]
        fathers, sons, grandsons, great_grandsons = self.maker.make()
        self.assertEqual(len(fathers), 1)
        self.assertEqual(len(sons), self.species_per_genus)
        self.assertEqual(len(grandsons), self.species_per_genus**2)
        self.assertEqual(len(great_grandsons), self.species_per_genus**3)
        
    def testGet(self) -> None:
        fathers: list[pd.DataFrame]
        sons: list[pd.DataFrame]
        grandsons: list[pd.DataFrame]
        great_grandsons: list[pd.DataFrame]
        fathers, sons, grandsons, great_grandsons = self.maker.get()
        self.assertEqual(len(fathers), 1)
        self.assertEqual(len(sons), self.species_per_genus)
        self.assertEqual(len(grandsons), self.species_per_genus**2)
        self.assertEqual(len(great_grandsons), self.species_per_genus**3)
        
    def tearDown(self) -> None:
        try:
            os.remove(self.test_db)
        except:
            pass
        
        
class TestTemplates(ut.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.test_db: str = 'tests/data/test_eidola.db'
        cls.generator: Templates = Templates(cls.test_db)
    
    def testShowTemplates(self) -> None:
        templates: dict[str, dict[str, list[str]]] = self.generator.show_templates()
        exp_templates: list[str] = ['philosophy', 'zettelkasten']
        self.assertEqual(list(templates.keys()), exp_templates)
    
    def testBuildTemplate(self) -> None:
        self.assertRaises(
            ValueError, self.generator.build_template, 
            'fake_template'
        )
        
        objects: dict[str, pd.DataFrame] = self.generator.build_template('philosophy')
        self.assertEqual(len(objects), 3)
        species: list[str] = list(objects.keys())
        exp_species: list[str] = ['philosophers', 'works', 'topics']
        self.assertEqual(species, exp_species)
        
        objects: dict[str, pd.DataFrame] = self.generator.build_template('zettelkasten')
        self.assertEqual(len(objects), 4)
        species = list(objects.keys())
        exp_species = ['scribes', 'libraries', 'cards', 'keywords']
        self.assertEqual(species, exp_species)
        
    @classmethod
    def tearDownClass(cls) -> None:
        try:
            os.remove(cls.test_db)
        except:
            pass


if __name__ == '__main__':
    ut.main()
