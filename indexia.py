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
    
    def add_index(self, cnxn, indexonym, scribe_id):
        dtype = self.tabula.columns['indices']
        index = self.get_or_create(cnxn, 'indices', dtype, 
                                   ['indexonym', 'scribe_id'], 
                                   [indexonym, scribe_id])
        
        return index
    
    def add_card(self, cnxn, index, created=None):
        dtype = self.tabula.columns['index']      
        (index_id, indexonym, _) = index
        
        if not created:
            created = terminal.ask_created()
        
        card = self.get_or_create(cnxn, indexonym, dtype,
                                  ['created', 'index_id'],
                                  [created, index_id])
        
        return card
    
    def add_logonym(self, cnxn, logonym, indexonym, card_id):
        dtype = self.tabula.columns['logonyms']
        f_key = 'FOREIGN KEY (card_id)'
        dtype[f_key] = dtype[f_key].format(indexonym=indexonym)
        logonym = self.get_or_create(cnxn, 'logonyms', dtype, 
                                     ['logonym', 'card_id'], 
                                     [logonym, card_id])
        
        return logonym
    
    def add_logonyms(self, cnxn, indexonym, card_id, logonyms=pd.DataFrame()):
        logonym = terminal.ask_name('logonym')
        logonym = self.add_logonym(cnxn, logonym, indexonym, card_id)
        logonyms = logonym if logonyms.empty else pd.concat([logonyms, logonym])
        self.cards_with_logonym(cnxn, logonym.loc[0, 'logonym'])
        add_more = terminal.ask_yes_no('add another logonym?')
        
        if add_more:
            return self.add_logonyms(cnxn, indexonym, card_id, logonyms)
        else:
            return logonyms
        
    def cards_in_index(self, cnxn, indexonym):
        select = querystore.select(indexonym, ['COUNT(card_id)'])
        cards = pd.read_sql(select, cnxn)
        print(f'{cards.shape[0]} cards in index {indexonym}.')
    
    def cards_with_logonym(self, cnxn, logonym):
        where = querystore.where(['logonym'], [logonym])
        select = querystore.select('logonyms', ['DISTINCT card_id'], where)
        cards = pd.read_sql(select, cnxn)
        print(f'{cards.shape[0]} cards have logonym {logonym}.')
        
        return cards
        
if __name__ == '__main__':
    with indexia() as ix:
        terminal.print_file_contents('data/indexia_lower.txt')
        cnxn = ix.open_cnxn(ix.db)
        
        pseudonym = terminal.ask_name('pseudonym')
        scribe = ix.add_scribe(cnxn, pseudonym)
        (scribe_id, pseudonym) = scribe.loc[0]
        print(f'welcome, {pseudonym}.')
        
        indexonym = terminal.ask_name('indexonym')
        index = ix.add_index(cnxn, indexonym, scribe_id).loc[0]
        ix.cards_in_index(cnxn, indexonym)
        
        card = ix.add_card(cnxn, index).loc[0]
        
        logonyms = ix.add_logonyms(cnxn, indexonym, card.card_id)