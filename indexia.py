# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:22:34 2022

@author: aphorikles
"""


from terminal import terminal
from queries import querystore
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
        result, pandas.DataFrame
            A dataframe of rows matching column & 
            value criteria.

        '''
        create = querystore.create(tablename, dtype)
        where = querystore.where(cols, vals)
        select = querystore.select(tablename, ['*'], where)
        cnxn.execute(create)
        cnxn.commit()
        result = pd.read_sql(select, cnxn)
        
        if result.empty and retry:
            insert = querystore.insert(tablename, [tuple(vals)], columns=cols)
            cnxn.execute(insert)
            cnxn.commit()
            return self.get_or_create(cnxn, tablename, dtype, cols, vals, retry=False)
        elif result.empty:
            raise ValueError(f'No rows in {tablename} where {where}.')
        
        return result
    
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
        
        card = self.get_or_create(cnxn, libronym, dtype,
                                  ['created', 'library_id'],
                                  [created, library_id])
        
        return card
    
    def add_logonym(self, cnxn, logonym, library, card):
        dtype = self.tabula.columns['logonyms']
        (library_id, libronym, _) = library.loc[0]
        (card_id, created, _) = card.loc[0]
        f_key = 'FOREIGN KEY (card_id)'
        dtype[f_key] = dtype[f_key].format(libronym=libronym)
        
        logonym = self.get_or_create(cnxn, 'logonyms', dtype, 
                                     ['logonym', 'card_id'], 
                                     [logonym, card_id])
        
        return logonym
    
    def add_logonyms(self, cnxn, library, card, logonyms=pd.DataFrame()):
        logonym = terminal.ask_name('logonym')
        logonym = self.add_logonym(cnxn, logonym, library, card)
        logonyms = logonym if logonyms.empty else pd.concat([logonyms, logonym])
        add_more = terminal.ask_yes_no('add another logonym?')
        
        if add_more:
            return self.add_logonyms(cnxn, library, card, logonyms)
        else:
            return logonyms
        
    def get_libraries(self, cnxn, scribe):
        (scribe_id, pseudonym) = scribe.loc[0]
        where = querystore.where(['scribe_id'], [scribe_id])
        select = querystore.select('libraries', ['*'], where)
        
        try:
            libraries = pd.read_sql(select, cnxn)
        except:
            libraries = pd.DataFrame()
        
        return libraries
        
    def get_cards(self, cnxn, library):
        (library_id, libronym, _) = library.loc[0]
        select = querystore.select(libronym, ['*'])
        
        try:
            cards = pd.read_sql(select, cnxn).sort_values(by='created')
        except:
            cards = pd.DataFrame()
        
        return cards
    
    def get_logonyms(self, cnxn, card):
        (card_id, created, _) = card.loc[0]
        where = querystore.where(['card_id'], [card_id])
        select = querystore.select('logonyms', ['*'], where)
        
        try:
            logonyms = pd.read_sql(select, cnxn)
        except:
            logonyms = pd.DataFrame()
        
        return logonyms
        
if __name__ == '__main__':
    with indexia() as ix:
        print(terminal.read('data/indexia_lower.txt'))
        cnxn = ix.open_cnxn(ix.db)
        
        pseudonym = terminal.ask_name('pseudonym')
        scribe = ix.add_scribe(cnxn, pseudonym)
        (scribe_id, pseudonym) = scribe.loc[0]
        
        print(f'welcome, {pseudonym}.')