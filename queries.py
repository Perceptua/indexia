# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 09:50:00 2022

@author: aherk
"""

class querystore:
    def create(tablename, columns):
        '''
        Get a SQL CREATE TABLE statement.

        Parameters
        ----------
        tablename : str
            Name of the table to create.
        columns : dict(str)
            Dict of columns to add to table. Keys are 
            column names, values are data types.

        Returns
        -------
        create : str
            A formatted SQL CREATE TABLE statement.

        '''
        columns = [f'{col} {dtype}' for col, dtype in columns.items()]
        columns = ','.join(columns)
        create = f'CREATE TABLE {tablename}'
        create = f'{create} ({columns})'
        
        return create
    
    def insert(tablename, values, columns=None):
        '''
        GET a SQL INSERT statement.

        Parameters
        ----------
        tablename : str
            Name of table into which values will be inserted.
        values : list(str) or list(tuple(str))
            A list of strings or tuples containing strings. 
            Should be equal-length values representing the 
            values to insert.

        Returns
        -------
        insert : str
            A formatted SQL INSERT statement.

        '''
        values = ','.join([f"{v}" for v in values])
        columns = f" ({','.join(columns)})" if columns else ''
        insert = f'INSERT INTO {tablename}{columns}'
        insert = f'{insert} VALUES {values}'
        
        return insert
    
    def select(tablename, columns, conditions):
        '''
        GET a SQL SELECT statement.

        Parameters
        ----------
        tablename : str
            Name of the table to select values from.
        columns : list(str)
            list of column names to select.
        conditions : str
            A SQL-formatted string of conditions to apply.

        Returns
        -------
        select : str
            A formatted SQL SELECT statement.

        '''
        columns = ','.join(columns)
        select = f'SELECT {columns} FROM {tablename} {conditions}'
        
        return select
            