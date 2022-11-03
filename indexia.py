# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:22:34 2022

@author: aphorikles
"""


from terminal import terminal
from queries import opera
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
        for db in self.cnxns:
            for cnxn in self.cnxns[db]:
                cnxn.close()
            
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
        
        if db in self.cnxns.keys():
            self.cnxns[db] += [cnxn]
        else:
            self.cnxns[db] = [cnxn]
        
        return cnxn
        
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
    
    def add_logonym(self, cnxn, logonym, card):
        dtype = self.tabula.columns['logonyms']
        (card_id, created, _) = card.loc[0]
        
        logonym = self.get_or_create(cnxn, 'logonyms', dtype, 
                                     ['logonym', 'card_id'], 
                                     [logonym, card_id])
        
        return logonym
    
    def add_logonyms(self, cnxn, card, logonyms=pd.DataFrame()):
        logonym = terminal.ask_name('logonym')
        logonym = self.add_logonym(cnxn, logonym, card)
        logonyms = logonym if logonyms.empty else pd.concat([logonyms, logonym])
        add_more = terminal.ask_yes_no('add another logonym?')
        
        if add_more:
            return self.add_logonyms(cnxn, card, logonyms)
        else:
            return logonyms
    
    
    
    ###########
    # getters #
    ###########
        
    def get_libraries(self, cnxn, scribe):
        (scribe_id, pseudonym) = scribe.loc[0]
        where = opera.where(['scribe_id'], [scribe_id])
        select = opera.select('libraries', ['*'], where)
        expected_columns = self.tabula.columns['libraries'].keys()
        libraries = opera.get_df(cnxn, select, expected_columns)
        
        return libraries
        
    def get_cards(self, cnxn, library, columns=['*']):
        (library_id, libronym, _) = library.loc[0]
        where = opera.where(['library_id'], [library_id])
        select = opera.select('cards', columns, where)
        expected_columns = self.tabula.columns['cards'].keys()
        cards = opera.get_df(cnxn, select, expected_columns)
        
        return cards
    
    def get_logonyms(self, cnxn, card):
        (card_id, created, _) = card.loc[0]
        where = opera.where(['card_id'], [card_id])
        select = opera.select('logonyms', ['*'], where)
        expected_columns = self.tabula.columns['logonyms'].keys()
        logonyms = opera.get_df(cnxn, select, expected_columns)
        
        return logonyms