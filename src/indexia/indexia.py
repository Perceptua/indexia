'''
indexia

Defines core operations on indexia objects.

'''
from indexia.inquiry import Inquiry
from indexia.schemata import Tabula
import os
import sqlite3
import pandas as pd


class Indexia:
    '''
    Core class for creating, modifying, & retrieving 
    indexia objects.
    
    '''
    def __init__(self, db=None):
        '''
        Create an indexia instance & build a path to 
        a default database file if one is not supplied.

        Parameters
        ----------
        db : str, optional
            Path to a database file. The default is None.

        Returns
        -------
        None.

        '''
        self.db = db if db else os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/indexia.db'
        )
        
        self.tabula = Tabula()
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
        
        print(open(os.path.join(
            os.path.abspath(__file__),
            '..',
            'resources/indexia_lower.txt'
        )).read())
        
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
        
        self.cnxns[db] = []
    
    def close_all_cnxns(self):
        '''
        Close all database connections.

        Returns
        -------
        None.

        '''
        for db in self.cnxns:
            self.close_cnxn(db)
            
    def get_df(self, cnxn, sql, expected_columns=None, raise_errors=False):
        '''
        Get result of SQL query as a pandas dataframe.
        In the event of an exception, return an empty
        dataframe.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            Connection to the database.
        sql : str
            SQL to be executed by pandas.read_sql.
        expected_columns : list(str), optional
            List of expected columns. If raise_errors is True 
            & the dataframe columns do not match expected_columns, 
            a ValueError is raised. The default is None.
        raise_errors : bool, optional
            Whether to raise exceptions encountered during 
            execution. The default is False.

        Raises
        ------
        error
            If raise_errors is True, raise any error encountered 
            during execution.

        Returns
        -------
        df : pandas.DataFrame
            A dataframe containing the results of the 
            SQL query.

        '''
        error = None
        
        try:
            df = pd.read_sql(sql, cnxn)
            
            if expected_columns and set(df.columns) != set(expected_columns):
                err_msg = ' '.join([
                    f'expected columns {expected_columns}.',
                    f'found {list(df.columns)}'
                ])
                
                error = ValueError(err_msg)
        
        except Exception as err:
            error = err
            df = pd.DataFrame(columns=expected_columns)
            
        if error and raise_errors:
            raise error
            
        return df
        
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
        create = Inquiry.create(tablename, dtype)
        where = Inquiry.where(cols, vals)
        select = Inquiry.select(tablename, ['*'], where)
        cnxn.execute(create)
        cnxn.commit()
        result = pd.read_sql(select, cnxn)
        
        if result.empty and retry:
            insert = Inquiry.insert(tablename, [tuple(vals)], columns=cols)
            cnxn.execute(insert)
            cnxn.commit()
            
            return self.get_or_create(
                cnxn, tablename, dtype, cols, vals, retry=False
            )
        
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
        where = Inquiry.where(['id'], [object_id])
        select = Inquiry.select(tablename, ['*'], where)
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
        where = Inquiry.where(['id'], [object_id])
        delete = Inquiry.delete(tablename, where)
        cursor = cnxn.cursor()
        cursor.execute(delete)
        cnxn.commit()
        rows_deleted = cursor.rowcount
        
        return rows_deleted
    
    def update(
            self, cnxn, tablename, 
            set_cols, set_values, 
            where_cols, where_values
        ):
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
        where = Inquiry.where(where_cols, where_values)
        update = Inquiry.update(tablename, set_cols, set_values, where)
        cursor = cnxn.cursor()
        cursor.execute(update)
        cnxn.commit()
        rows_updated = cursor.rowcount
        
        return rows_updated
    
    
    ##########
    # adders #
    ##########
    
    def add_scribe(self, cnxn, pseudonym):
        '''
        Get or create a scribe.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        pseudonym : str
            Pseudonym of the scribe.

        Returns
        -------
        scribe : pandas.DataFrame
            A single-row dataframe of scribe data.

        '''
        dtype = self.tabula.columns['scribes']
        
        scribe = self.get_or_create(
            cnxn, 'scribes', dtype, 
            ['pseudonym'], [pseudonym]
        )
        
        return scribe
    
    def add_library(self, cnxn, scribe, libronym):
        '''
        Get or create a library for a given scribe.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        scribe : pandas.DataFrame
            A single-row dataframe of scribe data.
        libronym : str
            Name of the library.

        Returns
        -------
        library : pandas.DataFrame
            A single-row dataframe of library data.

        '''
        dtype = self.tabula.columns['libraries']
        (scribe_id, pseudonym) = scribe.loc[0]
        
        library = self.get_or_create(
            cnxn, 'libraries', dtype, 
            ['libronym', 'scribe_id'], 
            [libronym, scribe_id]
        )
        
        return library
    
    def add_card(self, cnxn, library, created):
        '''
        Get or create a card in a given library.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        library : pandas.DataFrame
            A single-row dataframe of library data.
        created : str
            Card creation timestamp in yyyy-mm-dd-HH-MM 
            format.

        Returns
        -------
        card : pandas.DataFrame
            A single-row dataframe of card data.

        '''
        dtype = self.tabula.columns['cards']      
        (library_id, libronym, _) = library.loc[0]
        
        card = self.get_or_create(
            cnxn, 'cards', dtype,
            ['created', 'library_id'],
            [created, library_id]
        )
        
        return card
    
    def add_logonym(self, cnxn, card, logonym):
        '''
        Get or create a logonym on a given card.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        card : pandas.DataFrame
            A single-row dataframe of card data.
        logonym : str
            Logonym to be retrieved or created.

        Returns
        -------
        logonym : pandas.DataFrame
            A single-row dataframe of logonym data.

        '''
        dtype = self.tabula.columns['logonyms']
        (card_id, created, _) = card.loc[0]
        
        logonym = self.get_or_create(
            cnxn, 'logonyms', dtype, 
            ['logonym', 'card_id'], 
            [logonym, card_id]
        )
        
        return logonym
    
    
    ###########
    # getters #
    ###########
        
    def get_scribes(self, cnxn, pseudonym=None):
        '''
        Get all scribes. If a pseudonym is specified, 
        get only scribes having that pseudonym.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        pseudonym : str, optional
            Pseudonym to search for. The default is None.

        Returns
        -------
        scribes : pandas.DataFrame
            Dataframe of matching scribes.

        '''
        where = Inquiry.where(
            ['pseudonym'], [pseudonym]
        ) if pseudonym else ''
        
        select = Inquiry.select('scribes', ['*'], where)
        expected_columns = self.tabula.columns['scribes'].keys()
        scribes = self.get_df(cnxn, select, expected_columns)
        
        return scribes
    
    def get_libraries(self, cnxn, scribe, libronym=None):
        '''
        Get all libraries for a given scribe. If a libronym 
        is specified, get only libraries having that libronym.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        scribe : pandas.DataFrame
            A singe-row dataframe of scribe data.
        libronym : str, optional
            Libronym to search for. The default is None.

        Returns
        -------
        libraries : pandas.DataFrame
            Dataframe of matching libraries.

        '''
        (scribe_id, pseudonym) = scribe.loc[0]
        cols = ['scribe_id', 'libronym'] if libronym else ['scribe_id']
        vals = [scribe_id, libronym] if libronym else [scribe_id]
        where = Inquiry.where(cols, vals)
        select = Inquiry.select('libraries', ['*'], where)
        expected_columns = self.tabula.columns['libraries'].keys()
        libraries = self.get_df(cnxn, select, expected_columns)
        
        return libraries
        
    def get_cards(self, cnxn, library, created=None):
        '''
        Get all cards in a given library. If a created time 
        is specified, return only cards having that created 
        time.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        library : pandas.DataFrame
            A single-row dataframe of library data.
        created : str, optional
            Created time to search for. The default is None.

        Returns
        -------
        cards : pandas.DataFrame
            Dataframe of matching cards.

        '''
        (library_id, libronym, _) = library.loc[0]
        cols = ['library_id', 'created'] if created else ['library_id']
        vals = [library_id, created] if created else [library_id]
        where = Inquiry.where(cols, vals)
        select = Inquiry.select('cards', ['*'], where)
        expected_columns = self.tabula.columns['cards'].keys()
        cards = self.get_df(cnxn, select, expected_columns)
        
        return cards
    
    def get_logonyms(self, cnxn, card, logonym=None):
        '''
        Get all logonyms for a given card. If a logonym 
        is supplied, return only that logonym.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        card : pandas.DataFrame
            A single-row dataframe of card data.
        logonym : str, optional
            Optional logonym to search for. The default is None.

        Returns
        -------
        logonyms : pandas.DataFrame
            Dataframe of matching logonyms.

        '''
        (card_id, created, _) = card.loc[0]
        cols = ['card_id', 'logonym'] if logonym else ['card_id']
        vals = [card_id, logonym] if logonym else [card_id]
        where = Inquiry.where(cols, vals)
        select = Inquiry.select('logonyms', ['*'], where)
        expected_columns = self.tabula.columns['logonyms'].keys()
        logonyms = self.get_df(cnxn, select, expected_columns)
        
        return logonyms
    