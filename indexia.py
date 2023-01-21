# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:22:34 2022

@author: aphorikles
"""


from inquiry import opera
from schemata import tabula
import sqlite3
import pandas as pd



class indexia:
    def __init__(self, db='data/indexia.db'):
        self.db = db
        self.tabula = tabula()
        self.cnxns = {}
    
    def __enter__(self):
        '''
        Enable with _ as _ syntax.

        '''
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        '''
        Close all database connections on exit.

        '''
        self.close_all_cnxns()
            
    def open_cnxn(self, db):
        '''
        Open a connection to a database.

        Parameters
        ----------
        db : str
            The name of the database.

        Returns
        -------
        cnxn : sqlite3.Connection
            Connection to the database.

        '''
        cnxn = sqlite3.connect(db)
        cnxn.execute('PRAGMA foreign_keys = 1')
        print(open('data/indexia_lower.txt').read())
        
        if db in self.cnxns.keys():
            self.cnxns[db] += [cnxn]
        else:
            self.cnxns[db] = [cnxn]
        
        return cnxn
    
    def close_cnxn(self, db):
        '''
        Close connections to a database.

        Parameters
        ----------
        db : str
            Path to the database file.

        Returns
        -------
        None.

        '''
        for cnxn in self.cnxns[db]:
            cnxn.close()
    
    def close_all_cnxns(self):
        '''
        Close all database connections.

        Returns
        -------
        None.

        '''
        for db in self.cnxns:
            self.close_cnxn(db)
        
    def get_or_create(self, cnxn, tablename, dtype, cols, vals, retry=True):
        '''
        Get values from an existing table, or create 
        the table & insert them.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the table to query or create.
        dtype : dict(str)
            Dictionary with column name keys & SQL 
            datatype values.
        cols : list(str)
            List of table column names for query.
        vals : list(any)
            Values to query.
        retry : bool, optional
            Whether to try again on empty result. 
            The default is True.

        Raises
        ------
        ValueError
            Raised when no matching rows are found 
            & retry is False.

        Returns
        -------
        result : pandas.DataFrame
            A dataframe of rows matching column & 
            value criteria.

        '''
        create = opera.create(tablename, dtype)
        where = opera.where(cols, vals)
        select = opera.select(tablename, ['*'], where)
        cnxn.execute(create)
        cnxn.commit()
        result = pd.read_sql(select, cnxn)
        
        if result.empty and retry:
            insert = opera.insert(tablename, [tuple(vals)], columns=cols)
            cnxn.execute(insert)
            cnxn.commit()
            return self.get_or_create(cnxn, tablename, dtype, cols, vals, retry=False)
        elif result.empty:
            raise ValueError(f'No rows in {tablename} where {where}.')
        
        return result
    
    def get_by_id(self, cnxn, tablename, object_id):
        '''
        Get an object by its id.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the table to query.
        object_id : str or int
            Value of the object's id.

        Returns
        -------
        result : pandas.DataFrame
            A dataframe containing the query results.

        '''
        where = opera.where(['id'], [object_id])
        select = opera.select(tablename, ['*'], where)
        result = pd.read_sql(select, cnxn)
        
        return result
    
    def delete(self, cnxn, tablename, object_id):
        '''
        Delete an object from a table by ID.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the table from which to delete.
        object_id : int
            ID of the object to delete.

        Returns
        -------
        rows_deleted : int
            Count of rows affected by DELETE statement.

        '''
        where = opera.where(['id'], [object_id])
        delete = opera.delete(tablename, where)
        cursor = cnxn.cursor()
        cursor.execute(delete)
        cnxn.commit()
        rows_deleted = cursor.rowcount
        
        return rows_deleted
    
    def update(self, cnxn, tablename, set_cols, set_values, where_cols, where_values):
        '''
        Update values in a database table. Executes 
        a SQL statement of the form 
            UPDATE 
                {tablename}
            SET 
                {set_cols[0]} = {set_values[0]},
                {set_cols[1]} = {set_values[1]},
                ...
            WHERE 
                {where_cols[0]} = {where_values[0]} AND
                {where_cols[1]} = {where_values[1]} AND
                ...

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the table to update.
        set_cols : list(str)
            List of columns to update.
        set_values : list(any)
            Updated values for columns.
        where_cols : list(str)
            List of columns for WHERE condition.
        where_values : list(any)
            List of values for WHERE condition.

        Returns
        -------
        rows_updated : int
            Number of rows affected by update statement.

        '''
        where = opera.where(where_cols, where_values)
        update = opera.update(tablename, set_cols, set_values, where)
        cursor = cnxn.cursor()
        cursor.execute(update)
        cnxn.commit()
        rows_updated = cursor.rowcount
        
        return rows_updated
    
    
    
    ##########
    # adders #
    ##########
    
    def add_scribe(self, cnxn, pseudonym):
        dtype = self.tabula.columns['scribes']
        
        scribe = self.get_or_create(cnxn, 'scribes', dtype, 
                                    ['pseudonym'],
                                    [pseudonym])
        
        return scribe
    
    def add_library(self, cnxn, libronym, scribe):
        dtype = self.tabula.columns['libraries']
        (scribe_id, pseudonym) = scribe.loc[0]
        
        library = self.get_or_create(cnxn, 'libraries', dtype, 
                                     ['libronym', 'scribe_id'], 
                                     [libronym, scribe_id])
        
        return library
    
    def add_card(self, cnxn, library, created):
        dtype = self.tabula.columns['cards']      
        (library_id, libronym, _) = library.loc[0]
        
        card = self.get_or_create(cnxn, 'cards', dtype,
                                  ['created', 'library_id'],
                                  [created, library_id])
        
        return card
    
    def add_logonym(self, cnxn, card, logonym):
        dtype = self.tabula.columns['logonyms']
        (card_id, created, _) = card.loc[0]
        
        logonym = self.get_or_create(cnxn, 'logonyms', dtype, 
                                     ['logonym', 'card_id'], 
                                     [logonym, card_id])
        
        return logonym
    
    
    
    ###########
    # getters #
    ###########
        
    def get_scribes(self, cnxn, pseudonym=None):
        where = opera.where(['pseudonym'], [pseudonym]) if pseudonym else ''
        select = opera.select('scribes', ['*'], where)
        expected_columns = self.tabula.columns['scribes'].keys()
        scribes = opera.get_df(cnxn, select, expected_columns)
        
        return scribes
    
    def get_libraries(self, cnxn, scribe, libronym=None):
        (scribe_id, pseudonym) = scribe.loc[0]
        cols = ['scribe_id', 'libronym'] if libronym else ['scribe_id']
        vals = [scribe_id, libronym] if libronym else [scribe_id]
        where = opera.where(cols, vals)
        select = opera.select('libraries', ['*'], where)
        expected_columns = self.tabula.columns['libraries'].keys()
        libraries = opera.get_df(cnxn, select, expected_columns)
        
        return libraries
        
    def get_cards(self, cnxn, library, created=None):
        (library_id, libronym, _) = library.loc[0]
        cols = ['library_id', 'created'] if created else ['library_id']
        vals = [library_id, created] if created else [library_id]
        where = opera.where(cols, vals)
        select = opera.select('cards', ['*'], where)
        expected_columns = self.tabula.columns['cards'].keys()
        cards = opera.get_df(cnxn, select, expected_columns)
        
        return cards
    
    def get_logonyms(self, cnxn, card, logonym=None):
        (card_id, created, _) = card.loc[0]
        cols = ['card_id', 'logonym'] if logonym else ['card_id']
        vals = [card_id, logonym] if logonym else [card_id]
        where = opera.where(cols, vals)
        select = opera.select('logonyms', ['*'], where)
        expected_columns = self.tabula.columns['logonyms'].keys()
        logonyms = opera.get_df(cnxn, select, expected_columns)
        
        return logonyms
    
    def get_corpus(self, cnxn, scribe):
        (scribe_id, pseudonym) = scribe.loc[0]
        query_file = 'data/queries/corpus.sql'
        sql = open(query_file).read().format(scribe_id=scribe_id)
        corpus = opera.get_df(cnxn, sql)
        
        return corpus
    
    
if __name__ == '__main__':
    with indexia() as ix:
        cnxn = ix.open_cnxn(ix.db)
        scribe = ix.add_scribe(cnxn, 'aphorikles')
        corpus = ix.get_corpus(cnxn, scribe)