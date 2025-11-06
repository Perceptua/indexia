from indexia.indexia import Indexia
from indexia.inquiry import Tabula
from sqlite3 import Connection
import os
import pandas
import sqlite3
import unittest as ut


class TestIndexia(ut.TestCase):
    @classmethod        
    def setUpClass(cls) -> None:        
        cls.test_db: str = 'tests/data/test_indexia.db'
        
    @classmethod
    def getTestTables(cls) -> tuple[tuple[str, dict[str, str]], tuple[str, dict[str, str]]]:
        creator: tuple[str, dict[str, str]] = Tabula.get_creator_table(
            'creator', 'name'
        )
        
        creature: tuple[str, dict[str, str]] = Tabula.get_creature_table(
            'creator', 'creature', 'name'
        )
        
        return creator, creature

    def setUp(self) -> None:
        creator: tuple[str, dict[str, str]]
        creature: tuple[str, dict[str, str]]
        creator, creature = self.getTestTables()
        self.creator_table: str
        self.creator_dtype: dict[str, str]
        self.creator_table, self.creator_dtype = creator
        self.creature_table: str
        self.creature_dtype: dict[str, str]
        self.creature_table, self.creature_dtype = creature
        self.trait: str = 'name'
        self.creator_expr: str = 'father'
        self.creature_expr: str = 'son'
        
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            self.creator_data: pandas.DataFrame = ix.get_or_create(
                cnxn, self.creator_table, self.creator_dtype, 
                [self.trait], 
                [self.creator_expr]
            )
            
            self.creator_id: int = list(self.creator_data.iloc[0])[0]
            
            self.creature_data: pandas.DataFrame = ix.get_or_create(
                cnxn, self.creature_table, self.creature_dtype, 
                [self.trait, 'creator_id'], 
                [self.creature_expr, self.creator_id]
            )
            
            self.creature_id: int = list(self.creature_data.iloc[0])[0]
        
    def testOpenCnxn(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn_1: Connection = ix.open_cnxn(ix.db)
            cnxn_2: Connection = ix.open_cnxn(ix.db)
            
            self.assertEqual(len(ix.cnxns[self.test_db]), 2)
            self.assertIsInstance(cnxn_1, sqlite3.Connection)
            self.assertIsInstance(cnxn_2, sqlite3.Connection)
    
    def testCloseCnxn(self) -> None:
        with Indexia(self.test_db) as ix:
            ix.open_cnxn(ix.db)
            self.assertEqual(len(ix.cnxns[self.test_db]), 1)
            ix.close_cnxn(self.test_db)
            self.assertEqual(len(ix.cnxns[self.test_db]), 0)
    
    def testCloseAllCnxns(self) -> None:
        with Indexia(self.test_db) as ix:
            ix.open_cnxn(ix.db)
            self.assertEqual(len(ix.cnxns[self.test_db]), 1)
            ix.close_all_cnxns()
            
            for db in ix.cnxns:
                self.assertEqual(len(ix.cnxns[db]), 0)
                
    def testGetDF(self) -> None:
        creator_cols: list[str] = ['id', 'name']
        valid_sql: str = f'SELECT * FROM {self.creator_table};'
        invalid_sql: str = 'SELECT * FROM nonexistent_table;'
        
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            expected_columns: list[str] = []
            raise_errors = False

            df: pandas.DataFrame = ix.get_df(
                cnxn, valid_sql, expected_columns, raise_errors
            )

            self.assertIsInstance(df, pandas.DataFrame)
            self.assertEqual(list(df.columns), creator_cols)
            
            df = ix.get_df(cnxn, invalid_sql, expected_columns, raise_errors)
            self.assertIsInstance(df, pandas.DataFrame)
            self.assertEqual(list(df.columns), [])
            
            expected_columns = []
            raise_errors = True
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertIsInstance(df, pandas.DataFrame)
            self.assertEqual(list(df.columns), creator_cols)
            
            self.assertRaises(
                Exception, ix.get_df, 
                cnxn, invalid_sql, expected_columns, raise_errors
            )
            
            expected_columns = ['invalid_column']
            raise_errors = False
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertEqual(list(df.columns), creator_cols)
            
            expected_columns = ['invalid_column']
            raise_errors = True
            
            self.assertRaises(
                ValueError, ix.get_df, 
                cnxn, valid_sql, expected_columns, raise_errors
            )
            
            expected_columns = creator_cols
            raise_errors = True
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertEqual(list(df.columns), creator_cols)
            self.assertGreaterEqual(df.shape[0], 1)   
                
    def testGetOrCreate(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            creator_expr = 'neonymos'
            
            self.assertRaises(
                ValueError, ix.get_or_create, 
                cnxn, self.creator_table, self.creator_dtype, 
                [self.trait], [creator_expr], retry=False
            )
            
            creator_data: pandas.DataFrame = ix.get_or_create(
                cnxn, self.creator_table, self.creator_dtype, 
                [self.trait], [creator_expr], retry=True
            )
            
            self.assertIsInstance(creator_data, pandas.DataFrame)
            self.assertEqual(creator_data.shape[0], 1)
     
    def testDelete(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            deleted: int = ix.delete(cnxn, self.creator_table, self.creator_id)
            self.assertEqual(self.creator_id, deleted)
            
            self.assertRaises(
                ValueError, ix.get_or_create, 
                cnxn, self.creator_table, self.creator_dtype, 
                [self.trait], [self.creator_expr], retry=False
            )
            
    def testUpdate(self) -> None:        
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            rows_updated: int = ix.update(
                cnxn, self.creator_table, 
                [self.trait], ['pater'], 
                [self.trait], [self.creator_expr]
            )
                        
            updated: pandas.DataFrame = ix.get_or_create(
                cnxn, self.creator_table, self.creator_dtype, 
                ['id'], [self.creator_id]
            )
            
            self.assertEqual(rows_updated, 1)
            self.assertEqual(updated.loc[0, 'name'], 'pater')
            
    def testAddCreator(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            creator_data: pandas.DataFrame = ix.add_creator(
                cnxn, self.creator_table, self.trait, self.creator_expr
            )
            
            pandas.testing.assert_frame_equal(creator_data, self.creator_data)
            
            creator_data = ix.add_creator(
                cnxn, self.creator_table, self.trait, 'neonymos'
            )
            
            creator_id: int = 0
            creator_expr: str = ''
            creator_id, creator_expr = creator_data.iloc[0]
            self.assertEqual(creator_id, self.creator_id + 1)
            self.assertEqual(creator_expr, 'neonymos')
    
    def testAddCreature(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            creature_data: pandas.DataFrame = ix.add_creature(
                cnxn, self.creator_table, self.creator_data, 
                self.creature_table, self.trait, self.creature_expr
            )
            
            pandas.testing.assert_frame_equal(creature_data, self.creature_data)
            
            creature_data = ix.add_creature(
                cnxn, self.creator_table, self.creator_data,
                self.creature_table, self.trait, 'neonymos'
            )
            
            creature_id: int = 0
            creature_expr: str = ''
            creature_id, creature_expr, creator_id = creature_data.iloc[0]
            self.assertEqual(creator_id, self.creator_id)
            self.assertEqual(creature_id, self.creature_id + 1)
            self.assertEqual(creature_expr, 'neonymos')
                
    def testGetAllTables(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            table_list: list[str] = ix.get_all_tables(cnxn)
            
            self.assertListEqual(
                table_list, [self.creator_table, self.creature_table]
            )
    
    def testGetTableColumns(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)

            column_data: pandas.DataFrame = ix.get_table_columns(
                cnxn, self.creator_table
            )
            
            exp_column_data = pandas.DataFrame(data={
                'column_name': ['id', 'name'],
                'data_type': ['INTEGER', 'TEXT'],
                'not_null': [1, 1],
                'is_pk': [1, 0]
            })
            
            pandas.testing.assert_frame_equal(column_data, exp_column_data)
            
    def testGetTrait(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            trait: str = ix.get_trait(cnxn, self.creator_table)
            self.assertEqual(trait, self.trait)
        
            trait = ix.get_trait(cnxn, self.creature_table)
            self.assertEqual(trait, self.trait)
        
            self.assertRaises(ValueError, ix.get_trait, cnxn,'exp_fail')
    
    def testGetByTrait(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            creator_retrieved: pandas.DataFrame = ix.get_by_trait(
                cnxn, self.creator_table, self.creator_expr
            )
            
            pandas.testing.assert_frame_equal(self.creator_data, creator_retrieved)
            
            expect_empty: pandas.DataFrame = ix.get_by_trait(
                cnxn, self.creator_table, f'{self.creator_expr}_empty'
            )
            
            self.assertTrue(expect_empty.empty)
                
    def testGetByID(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            
            creator_retrieved: pandas.DataFrame = ix.get_by_id(
                cnxn, self.creator_table, self.creator_id
            )
            
            pandas.testing.assert_frame_equal(self.creator_data, creator_retrieved)
            
            expect_empty: pandas.DataFrame = ix.get_by_id(
                cnxn, self.creator_table, self.creator_id + 1
            )
            
            self.assertTrue(expect_empty.empty)
    
    def testGetCreatorGenus(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            genus: str | None = ix.get_creator_genus(cnxn, self.creature_table)
            self.assertEqual(self.creator_table, genus)
            
    def testGetCreatureSpecies(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            species: list[str] = ix.get_creature_species(cnxn, self.creator_table)
            exp_species: list[str] = ['creature']
            self.assertEqual(species, exp_species)
            
            species = ix.get_creature_species(cnxn, self.creature_table)
            exp_species = []
            self.assertEqual(species, exp_species)
    
    def testGetCreator(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            creator_genus: str = ''
            creator_data: pandas.DataFrame = pandas.DataFrame()

            creator_genus, creator_data = ix.get_creator(
                cnxn, self.creature_table, self.creature_data
            )[0]
            
            exp_genus: str | None = ix.get_creator_genus(
                cnxn, self.creature_table
            )

            self.assertEqual(creator_genus, exp_genus)
            pandas.testing.assert_frame_equal(creator_data, self.creator_data)
            
            expect_empty: list[tuple[str, pandas.DataFrame]] = ix.get_creator(
                cnxn, self.creator_table, self.creator_data
            )
            
            self.assertEqual(len(expect_empty), 0)
            
    def testGetCreatures(self) -> None:
        with Indexia(self.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)  
            
            creatures: list[tuple[str, pandas.DataFrame]] = ix.get_creatures(
                cnxn, self.creator_table, self.creator_data
            )
            
            exp_creatures: list[tuple[str, pandas.DataFrame]] = [
                ('creature', pandas.DataFrame(data={
                    'id': [1], 'name': ['son'], 'creator_id': [1]
                }))
            ]
            
            species: str = ''
            members: pandas.DataFrame = pandas.DataFrame()
            exp_species: str = ''
            exp_members: pandas.DataFrame = pandas.DataFrame()
            species, members = creatures[0]
            exp_species, exp_members = exp_creatures[0]
            self.assertEqual(species, exp_species)
            pandas.testing.assert_frame_equal(members, exp_members)
            
            exp_empty: list[tuple[str, pandas.DataFrame]] = ix.get_creatures(
                cnxn, self.creature_table, self.creature_data
            )
            
            self.assertEqual(len(exp_empty), 0)
    
    def tearDown(self) -> None:
        try:
            os.remove(self.test_db)
        except:
            pass


if __name__ == '__main__':
    ut.main()