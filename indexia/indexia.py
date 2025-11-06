'''
Defines core operations on indexia objects.

'''
from indexia.inquiry import Inquiry, Tabula
from typing import Any
import os
import sqlite3
import pandas


class Indexia:
    '''
    Core class for creating, modifying, & retrieving 
    indexia objects.
    
    '''
    def __init__(
        self, db: str | None = None
    ) -> None:
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
        self.cnxns: dict[str, list[sqlite3.Connection]] = {}
        
        self.db: str = db if db else os.path.join(
            os.path.abspath(__file__),
            '..', 'data', 'indexia.db'
        )
    
    def __enter__(
        self
    ) -> 'Indexia':
        '''
        Enable with _ as _ syntax.

        '''
        return self


    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any
    ) -> None:
        '''
        Close all database connections on exit.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            The type of the exception that was raised
        exc_value : BaseException | None
            The instance of the exception that was raised
        traceback : Any
            The traceback if an exception was raised

        Returns
        -------
        None

        '''
        self.close_all_cnxns()
            
    def open_cnxn(
        self, db: str
    ) -> sqlite3.Connection:
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
        cnxn: sqlite3.Connection = sqlite3.connect(db)
        cnxn.execute('PRAGMA foreign_keys = 1')
        
        if db in self.cnxns.keys():
            self.cnxns[db] += [cnxn]
        else:
            self.cnxns[db] = [cnxn]
        
        return cnxn
    
    def close_cnxn(
        self, db: str
    ) -> None:
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
    
    def close_all_cnxns(
        self
    ) -> None:
        '''
        Close all database connections.

        Returns
        -------
        None.

        '''
        for db in self.cnxns:
            self.close_cnxn(db)
            
    def get_df(
        self,
        cnxn: sqlite3.Connection,
        sql: str,
        expected_columns: list[str]=[],
        raise_errors: bool=False
    ) -> pandas.DataFrame:
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
        expected_columns : list[str], optional
            List of expected columns. If raise_errors is True 
            & the dataframe columns do not match expected_columns, 
            a ValueError is raised. The default is [].
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
        error: ValueError | Exception | None = None
        
        try:
            df: pandas.DataFrame = pandas.read_sql(sql, cnxn) # type: ignore
            
            if expected_columns and set(df.columns) != set(expected_columns):
                err_msg: str = ' '.join([
                    f'expected columns {expected_columns}.',
                    f'found {list(df.columns)}'
                ])
                
                error = ValueError(err_msg)
        
        except Exception as err:
            error = err
            df = pandas.DataFrame(columns=expected_columns)
            
        if error and raise_errors:
            raise error
            
        return df
            
    def get_or_create(
        self,
        cnxn: sqlite3.Connection,
        tablename: str,
        dtype: dict[str, str], 
        cols: list[str],
        vals: list[Any],
        retry: bool = True
    ) -> pandas.DataFrame:
        '''
        Get entities from an existing table, or create 
        the table & (optionally) insert them.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the database table. If the table does 
            not exist, it will be created.
        dtype : dict[str, str]
            Dict of table columns & column data types.
        cols : list[str]
            Columns to be used in SELECT statement.
        vals : list[str]
            Values to be used in SELECT statement.
        retry : bool, optional
            If true & SELECT returns an empty result, 
            INSERT the specifies values & try again.
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
        create: str = Inquiry.create(tablename, dtype)
        cnxn.execute(create)
        cnxn.commit()
        
        where: str = Inquiry.where(cols, vals)
        select: str = Inquiry.select(tablename, ['*'], where)
        result: pandas.DataFrame = self.get_df(cnxn, select)
        
        if result.empty and retry:
            insert: str = Inquiry.insert(tablename, [tuple(vals)], columns=cols)
            cnxn.execute(insert)
            cnxn.commit()
            
            return self.get_or_create(
                cnxn, tablename, dtype, cols, vals, retry=False
            )
        
        elif result.empty:
            raise ValueError(f'No rows in {tablename} where {where}.')
        
        return result
                
    def delete(
        self,
        cnxn: sqlite3.Connection,
        species: str,
        entity_id: int
    ) -> int:
        '''
        Delete an entity from a table by ID.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        species : str
            Name of the table from which to delete.
        entity_id : int
            ID of the entity to delete.

        Returns
        -------
        rows_deleted : int
            Count of rows affected by DELETE statement.

        '''
        where: str = Inquiry.where(['id'], [entity_id])
        delete: str = Inquiry.delete(species, where)
        cursor: sqlite3.Cursor = cnxn.cursor()
        cursor.execute(delete)
        cnxn.commit()
        rows_deleted: int = cursor.rowcount
        
        return rows_deleted
    
    def update(
        self,
        cnxn: sqlite3.Connection,
        tablename: str,
        set_cols: list[str],
        set_vals: list[Any],
        where_cols: list[str],
        where_vals: list[Any]
    ) -> int:
        '''
        Update values in a database table. Executes a SQL statement 
        of the form
        
        UPDATE 
            {tablename}
        SET 
            {set_cols[0]} = {set_vals[0]},
            {set_cols[1]} = {set_vals[1]},
            ...
        WHERE 
            {where_cols[0]} = {where_vals[0]} AND
            {where_cols[1]} = {where_vals[1]} AND
            ...

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the table to update.
        set_cols : list[str]
            List of columns to update.
        set_vals : list[Any]
            Updated values for columns.
        where_cols : list[str]
            List of columns for WHERE condition.
        where_vals : list[Any]
            List of values for WHERE condition.

        Returns
        -------
        rows_updated : int
            Number of rows affected by update statement.

        '''
        where: str = Inquiry.where(where_cols, where_vals)
        update: str = Inquiry.update(tablename, set_cols, set_vals, where)
        cursor: sqlite3.Cursor = cnxn.cursor()
        cursor.execute(update)
        cnxn.commit()
        rows_updated: int = cursor.rowcount
        
        return rows_updated
    
    
    ##########
    # adders #
    ##########
    
    def add_creator(
        self,
        cnxn: sqlite3.Connection,
        genus: str,
        trait: str,
        expr: str
    ) -> pandas.DataFrame:
        '''
        Get or create a creator entity.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table to be retrieved 
            or created.
        trait : str
            Name of the creator's text attribute.
        expr : str
            Value of the creator's text attribute.

        Returns
        -------
        creator : pandas.DataFrame
            A single-row dataframe of creator entity data.

        '''
        creator_table: tuple[str, dict[Any, str]] = Tabula.get_creator_table(
            genus, trait
        )

        dtype: dict[str, Any | str] = creator_table[1]
        creator: pandas.DataFrame = self.get_or_create(cnxn, genus, dtype, [trait], [expr])
        
        return creator
    
    def add_creature(
        self,
        cnxn: sqlite3.Connection,
        genus: str,
        creator: pandas.DataFrame, 
        species: str,
        trait: str,
        expr: str
    ) -> pandas.DataFrame:
        '''
        Get or create a creature of a given creator.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table.
        creator : pandas.DataFrame
            A single-row dataframe of creator entity data.
        species : str
            Name of the creature (child) table to be retrieved 
            or created.
        trait : str
            Name of the creature's text attribute.
        expr : str
            Value of the creature's text attribute.

        Returns
        -------
        creature : pandas.DataFrame
            A single-row dataframe of creature entity data.

        '''
        creator_id: int = list(creator.id)[0]

        creature_table: tuple[str, dict[str, str]] = Tabula.get_creature_table(
            genus, species, trait
        )

        dtype: dict[str, Any | str] = creature_table[1]

        creature: pandas.DataFrame = self.get_or_create(
            cnxn, species, dtype, [trait, f'{genus}_id'], [expr, creator_id]
        )
        
        return creature
    
    
    ###########
    # getters #
    ###########
    
    def get_all_tables(
        self,
        cnxn: sqlite3.Connection
    ) -> list[str]:
        '''
        Get all tables in the instance database.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.

        Returns
        -------
        tables : list[str]
            List of table names in the database.

        '''
        where: str = Inquiry.where(['type'], ['table'])
        where = f"{where} AND name NOT LIKE 'sqlite_%'"
        sql: str = Inquiry.select('sqlite_schema', ['name'], where)
        tables: list[Any] = list(self.get_df(cnxn, sql).name)
        
        return tables
    
    def get_table_columns(
        self,
        cnxn: sqlite3.Connection,
        tablename: str
    ) -> pandas.DataFrame:
        '''
        Get columns of a database table.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        tablename : str
            Name of the database table.

        Returns
        -------
        columns : pandas.DataFrame
            Dataframe describing table columns.

        '''
        pragma: str = f'PRAGMA TABLE_INFO({tablename});'
        
        columns: pandas.DataFrame = self.get_df(cnxn, pragma)[
            ['name', 'type', 'notnull', 'pk']
        ].rename(columns={
            'name': 'column_name',
            'type': 'data_type',
            'notnull': 'not_null',
            'pk': 'is_pk'
        })
        
        return columns
    
    def get_trait(self,
        cnxn: sqlite3.Connection,
        kind: str
    ) -> str:
        '''
        Gets the trait (attribute) column of the given 
        kind.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        kind : str
            Name of the table.

        Raises
        ------
        ValueError
            If no trait column is identified, or if more 
            than one trait column is identified, raise a 
            ValueError.

        Returns
        -------
        trait : str
            Name of the trait column.

        '''
        columns: pandas.Series[str] = self.get_table_columns(
            cnxn, kind
        ).column_name

        traits: list[str] = [
            c for c in columns if c != 'id' and not c.endswith('_id')
        ]
        
        if not traits or len(traits) > 1:
            err_msg: str = 'Found multiple trait columns'
            err_msg = err_msg if traits else 'Found no trait column'
            err_msg = f'{err_msg} for {kind}.'
            raise ValueError(err_msg)
            
        trait: str = traits[0]
        
        return trait
    
    def get_by_trait(
        self,
        cnxn: sqlite3.Connection,
        kind: str,
        expr: str
    ) -> pandas.DataFrame:
        '''
        Get being(s) by the text attribute value.
        
        Note that since values of the trait column need not be 
        unique, it is possible that the dataframe returned 
        will contain more than one being.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        kind : str
            Name of the table to query.
        expr : str
            Value of the being's trait (text attribute).

        Returns
        -------
        being : pandas.DataFrame
            Dataframe of one or more beings.

        '''
        trait: str = self.get_trait(cnxn, kind)
        where: str = Inquiry.where([trait], [expr])
        select: str = Inquiry.select(kind, ['*'], where)
        being: pandas.DataFrame = self.get_df(cnxn, select)
        
        return being
    
    def get_by_id(
        self,
        cnxn: sqlite3.Connection,
        kind: str,
        being_id: int
    ) -> pandas.DataFrame:
        '''
        Get an entity by its id.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        kind : str
            Name of the table to query.
        being_id : int
            Value of the entity's id.

        Returns
        -------
        being : pandas.DataFrame
            Dataframe of being data.

        '''
        where: str = Inquiry.where(['id'], [being_id])
        select: str = Inquiry.select(kind, ['*'], where)
        being: pandas.DataFrame = self.get_df(cnxn, select)
        
        return being
    
    def get_creator_genus(
        self,
        cnxn: sqlite3.Connection,
        species: str
    ) -> str | None:
        '''
        Get table name of creator (parent) table.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        species : str
            Name of the creature (child) table.

        Raises
        ------
        ValueError
            If more than one creator table is found, 
            raise a ValueError. Each creature table 
            should have one & only one creator.

        Returns
        -------
        genus : str | None
            Name of the creator (parent) table.

        '''
        pragma: str = f'PRAGMA FOREIGN_KEY_LIST({species});'
        foreign_keys: pandas.DataFrame = self.get_df(cnxn, pragma)
        genus: str | None = None
        
        if foreign_keys.shape[0] > 1:
            msg: str = ' '.join([
                'Data integrity error:',
                f'{species} shows more than one creator',
                f'({str(list(foreign_keys.table))}).'
            ])
            
            raise ValueError(msg)
            
        elif not foreign_keys.empty:
            table_names: list[Any] = foreign_keys.table.tolist()
            genus = str(table_names[0])
            
        return genus
    
    def get_creature_species(
        self,
        cnxn: sqlite3.Connection,
        genus: str
    ) -> list[str]:
        '''
        Get types of all creatures with a given creator genus.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table.

        Returns
        -------
        species : list[str]
            List of creature (child) table names.

        '''
        tables: list[str] = self.get_all_tables(cnxn)
        
        species: list[str] = [
            t for t in tables if self.get_creator_genus(cnxn, t) == genus
        ]
        
        return species
                
    def get_creator(
        self,
        cnxn: sqlite3.Connection,
        species: str,
        creature: pandas.DataFrame
    ) -> list[tuple[str, pandas.DataFrame]]:
        '''
        Get the creator of a given creature.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        species : str
            Name of the creature (child) table.
        creature : pandas.DataFrame
            A single-row dataframe of creature entity data.

        Returns
        -------
        creator : list[tuple[str, pandas.DataFrame]]
            List containing a single tuple of (creator table name, creator data).

        '''
        genus: str | None = self.get_creator_genus(cnxn, species)
        creator: list[tuple[str, pandas.DataFrame]] = []
        
        if genus:
            creator_id: int = creature[f'{genus}_id'].values[0]
            where: str = Inquiry.where(['id'], [creator_id])
            select: str = Inquiry.select(genus, ['*'], where)
            creator = [(genus, self.get_df(cnxn, select))]
        
        return creator
    
    def get_creatures(
        self, cnxn: sqlite3.Connection,
        genus: str,
        creator: pandas.DataFrame
    ) -> list[tuple[str, pandas.DataFrame]]:
        '''
        Get all creatures of a given creator.

        Parameters
        ----------
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table.
        creator : pandas.DataFrame
            A single-row dataframe of creator entity data.

        Returns
        -------
        creatures : list[tuple[str, pandas.DataFrame]]
            List of two-tuples whose first entry is the 
            name of the creature (child) table, & whose 
            second entry is a dataframe of creature data.

        '''
        creator_id: int = creator.id.values[0]
        species: list[str] = self.get_creature_species(cnxn, genus)
        creatures: list[Any] = []
        
        for s in species:
            where: str = Inquiry.where([f'{genus}_id'], [creator_id])
            select: str = Inquiry.select(s, ['*'], where)
            
            members: pandas.DataFrame = self.get_df(cnxn, select)
            
            creatures += [(s, members)]
            
        return creatures
            
    