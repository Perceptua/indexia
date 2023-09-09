from datetime import datetime as dt
from indexia.indexia import Indexia
import os
import pandas as pd
import sqlite3
import unittest as ut


class TestIndexia(ut.TestCase):
    @classmethod        
    def setUpClass(cls):
        cls.created = dt.now().strftime('%Y-%m-%d-%H-%M')
        
        cls.test_db = os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/test.db'
        )
        
    @classmethod
    def makeTestObjects(cls):
        with Indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            cls.scribe = ix.add_scribe(cnxn, 'test')
            cls.library = ix.add_library(cnxn, cls.scribe, 'test')
            cls.card = ix.add_card(cnxn, cls.library, created=cls.created)
            cls.logonym = ix.add_logonym(cnxn, cls.card, 'test')
        
        (cls.scribe_id, cls.pseudonym) = cls.scribe.loc[0]
        
        (cls.library_id, 
         cls.libronym, 
         cls.scribe_id_library) = cls.library.loc[0]
        
        (cls.card_id,
         cls.created_card, 
         cls.library_id_card) = cls.card.loc[0]
        
        (cls.logonym_id, 
         cls.logonym, 
         cls.card_id_logonym) = cls.logonym.loc[0]
        
    def testOpenCnxn(self):
        with Indexia(self.test_db) as ix:
            cnxn_1 = ix.open_cnxn(ix.db)
            cnxn_2 = ix.open_cnxn(ix.db)
            
            self.assertEqual(len(ix.cnxns[self.test_db]), 2)
            self.assertIsInstance(cnxn_1, sqlite3.Connection)
            self.assertIsInstance(cnxn_2, sqlite3.Connection)
    
    def testCloseCnxn(self):
        with Indexia(self.test_db) as ix:
            ix.open_cnxn(ix.db)
            self.assertEqual(len(ix.cnxns[self.test_db]), 1)
            ix.close_cnxn(self.test_db)
            self.assertEqual(len(ix.cnxns[self.test_db]), 0)
    
    def testCloseAllCnxns(self):
        with Indexia(self.test_db) as ix:
            ix.open_cnxn(ix.db)
            self.assertEqual(len(ix.cnxns[self.test_db]), 1)
            ix.close_all_cnxns()
            
            for db in ix.cnxns:
                self.assertEqual(len(ix.cnxns[db]), 0)
    
    def testGetDF(self):
        self.makeTestObjects()
        scribe_cols = ['id', 'pseudonym']
        valid_sql = 'SELECT * FROM scribes;'
        invalid_sql = 'SELECT * FROM nonexistent_table;'
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            
            expected_columns = None
            raise_errors = False
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(list(df.columns), scribe_cols)
            df = ix.get_df(cnxn, invalid_sql, expected_columns, raise_errors)
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(list(df.columns), [])
            
            expected_columns = None
            raise_errors = True
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertIsInstance(df, pd.DataFrame)
            self.assertEqual(list(df.columns), scribe_cols)
            self.assertRaises(
                pd.io.sql.DatabaseError, ix.get_df, 
                cnxn, invalid_sql, expected_columns, raise_errors
            )
            
            expected_columns = ['invalid_column']
            raise_errors = False
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertEqual(list(df.columns), scribe_cols)
            
            expected_columns = ['invalid_column']
            raise_errors = True
            self.assertRaises(
                ValueError, ix.get_df, 
                cnxn, valid_sql, expected_columns, raise_errors
            )
            
            expected_columns = scribe_cols
            raise_errors = True
            df = ix.get_df(cnxn, valid_sql, expected_columns, raise_errors)
            self.assertEqual(list(df.columns), scribe_cols)
            self.assertGreaterEqual(df.shape[0], 1)
            
    def testGetOrCreate(self):
        self.makeTestObjects()
        tablename = 'scribes'        
        cols = ['id', 'pseudonym']
        vals = [self.scribe_id + 2, 'test2']
        retry = False
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            dtype = ix.tabula.columns['scribes']
                        
            self.assertRaises(
                ValueError, ix.get_or_create, cnxn,
                tablename, dtype, cols, vals, retry
            )
            
            retry = True
            
            created = ix.get_or_create(
                cnxn, tablename, dtype, 
                cols, vals, retry
            )
            
            self.assertEqual(list(created.loc[0]), vals)
            
            retry = False
            
            created = ix.get_or_create(
                cnxn, tablename, dtype, 
                cols, vals, retry
            )
            
            self.assertEqual(list(created.loc[0]), vals)
            
    def testGetByID(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.get_by_id(cnxn, 'scribes', self.scribe_id)
            scribe_id, pseudonym = scribe.loc[0]
            self.assertEqual(scribe_id, self.scribe_id)
            self.assertEqual(pseudonym, 'test')
            no_scribe = ix.get_by_id(cnxn, 'scribes', 1000)
            self.assertTrue(no_scribe.empty)
    
    def testDelete(self):
        self.makeTestObjects()
        rows_deleted = 0
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            
            # expect 1
            rows_deleted += ix.delete(cnxn, 'logonyms', self.logonym_id)
            rows_deleted += ix.delete(cnxn, 'cards', self.card_id)
            rows_deleted += ix.delete(cnxn, 'scribes', self.scribe_id)
            self.assertEqual(rows_deleted, 3)
            
            # expect 0 (already deleted by ON_DELETE=CASCADE constraint)
            rows_deleted += ix.delete(cnxn, 'libraries', self.library_id)
            library = ix.get_by_id(cnxn, 'libraries', self.library_id)
            self.assertEqual(rows_deleted, 3)
            self.assertTrue(library.empty)
    
    def testUpdate(self):
        self.makeTestObjects()
        rows_updated = 0
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            
            rows_updated += ix.update(
                cnxn, 'scribes', 
                ['pseudonym'], ['testUpdate'], 
                ['id'], [self.scribe_id]
            )
            
            _, updated = ix.get_by_id(cnxn, 'scribes', self.scribe_id).loc[0]
            self.assertEqual(rows_updated, 1) 
            self.assertEqual(updated, 'testUpdate')
    
    def testAddScribe(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            add_existing = ix.add_scribe(cnxn, self.pseudonym)
            result = ix.get_by_id(cnxn, 'scribes', self.scribe_id)
            pd.testing.assert_frame_equal(add_existing, result)
            
            add_new = ix.add_scribe(cnxn, 'testNew')
            new_id, new_pseudonym = add_new.loc[0]
            
            result = ix.get_or_create(
                cnxn, 'scribes', ix.tabula.columns['scribes'],
                ['id', 'pseudonym'], [new_id, new_pseudonym],
                retry=False
            )
            
            pd.testing.assert_frame_equal(add_new, result)
    
    def testAddLibrary(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            add_existing = ix.add_library(cnxn, self.scribe, self.libronym)
            result = ix.get_by_id(cnxn, 'libraries', self.library_id)
            pd.testing.assert_frame_equal(add_existing, result)
            
            add_new = ix.add_library(cnxn, self.scribe, 'testNew')
            new_id, new_libronym, new_scribe_id = add_new.loc[0]
            
            result = ix.get_or_create(
                cnxn, 'libraries', ix.tabula.columns['libraries'],
                ['id', 'libronym', 'scribe_id'], 
                [new_id, new_libronym, new_scribe_id],
                retry=False
            )
            
            pd.testing.assert_frame_equal(add_new, result)
    
    def testAddCard(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            add_existing = ix.add_card(cnxn, self.library, self.created)
            result = ix.get_by_id(cnxn, 'cards', self.card_id)
            pd.testing.assert_frame_equal(add_existing, result)
            
            add_new = ix.add_card(cnxn, self.library, '2023-09-07-12-30')
            new_id, new_created, new_library_id = add_new.loc[0]
            
            result = ix.get_or_create(
                cnxn, 'cards', ix.tabula.columns['cards'],
                ['id', 'created', 'library_id'], 
                [new_id, new_created, new_library_id],
                retry=False
            )
            
            pd.testing.assert_frame_equal(add_new, result)
    
    def testAddLogonym(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            add_existing = ix.add_logonym(cnxn, self.card, self.logonym)
            result = ix.get_by_id(cnxn, 'logonyms', self.logonym_id)
            pd.testing.assert_frame_equal(add_existing, result)
            
            add_new = ix.add_logonym(cnxn, self.card, 'testNew')
            new_id, new_logonym, new_card_id = add_new.loc[0]
            
            result = ix.get_or_create(
                cnxn, 'logonyms', ix.tabula.columns['logonyms'],
                ['id', 'logonym', 'card_id'], 
                [new_id, new_logonym, new_card_id],
                retry=False
            )
            
            pd.testing.assert_frame_equal(add_new, result)
    
    def testGetScribes(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            ix.add_scribe(cnxn, 'test2')
            all_scribes = ix.get_scribes(cnxn)
            new_scribe = ix.get_scribes(cnxn, pseudonym='test2')
            self.assertGreaterEqual(all_scribes.shape[0], 2)
            self.assertEqual(new_scribe.shape[0], 1)
    
    def testGetLibraries(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            ix.add_library(cnxn, self.scribe, 'test2')
            all_libraries = ix.get_libraries(cnxn, self.scribe)
            
            new_library = ix.get_libraries(
                cnxn, self.scribe, libronym='test2'
            )
            
            self.assertGreaterEqual(all_libraries.shape[0], 2)
            self.assertEqual(new_library.shape[0], 1)
    
    def testGetCards(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            ix.add_card(cnxn, self.library, '2023-09-08-16-30')
            all_cards = ix.get_cards(cnxn, self.library)
            
            new_card = ix.get_cards(
                cnxn, self.library, created='2023-09-08-16-30'
            )
            
            self.assertGreaterEqual(all_cards.shape[0], 2)
            self.assertEqual(new_card.shape[0], 1)
    
    def testGetLogonyms(self):
        self.makeTestObjects()
        
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            ix.add_logonym(cnxn, self.card, 'test2')
            all_logonyms = ix.get_logonyms(cnxn, self.card)
            
            new_logonym = ix.get_logonyms(
                cnxn, self.card, logonym='test2'
            )
            
            self.assertGreaterEqual(all_logonyms.shape[0], 2)
            self.assertEqual(new_logonym.shape[0], 1)     
    
    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls.test_db)
        except:
            pass


if __name__ == '__main__':
    ut.main()