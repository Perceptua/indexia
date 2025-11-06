'''
Generate SQL for indexia database oprerations.

'''
from typing import Any

class Inquiry:
    '''
    Generate SQL strings from dynamic inputs. 
    
    '''
    @staticmethod
    def create(
        tablename: str,
        columns: dict[str, str]
    ) -> str:
        '''
        Get a SQL CREATE TABLE statement.

        Parameters
        ----------
        tablename : str
            Name of the table to create.
        columns : dict[str, str]
            Dict of columns to add to table. Keys are 
            column names, values are data types.

        Returns
        -------
        create : str
            A formatted SQL CREATE TABLE statement.

        '''
        column_list: list[str] = [
            f'{col} {dtype}' for col, dtype in columns.items()
        ]
        
        column_str: str = ','.join(column_list)
        create: str = f'CREATE TABLE IF NOT EXISTS {tablename}'
        create = f'{create} ({column_str})'
        
        return create
    
    @staticmethod
    def insert(
        tablename: str,
        values: list[Any] | list[tuple[Any, ...]],
        columns: list[str] | None = None
    ) -> str:
        '''
        GET a SQL INSERT statement.

        Parameters
        ----------
        tablename : str
            Name of table into which values will be inserted.
        values : list[Any] | list[tuple[Any, ...]]
            A list of values or tuples containing values. 
            Entries represent values to insert, & should be of equal length.
        columns : list[str] | None, optional
            List of column names. The default is None.

        Returns
        -------
        insert : str
            A formatted SQL INSERT statement.

        '''
        value_str: str = ','.join(
            '(' + ','.join(f"'{j}'" for _, j in enumerate(v)) + ')' 
            for v in values
        )
        
        column_str: str = f" ({','.join(columns)})" if columns else ''
        insert: str = f'INSERT INTO {tablename}{column_str}'
        insert = f'{insert} VALUES {value_str}'
        
        return insert
    
    @staticmethod
    def select(
        tablename: str,
        columns: list[str],
        conditions: str = ''
    ) -> str:
        '''
        GET a SQL SELECT statement.

        Parameters
        ----------
        tablename : str
            Name of the table from which to select values.
        columns : list[str]
            list of column names to select.
        conditions : str, optional
            A SQL-formatted string of conditions. The default is ''.

        Returns
        -------
        select : str
            A formatted SQL SELECT statement.

        '''
        column_str: str = ','.join(columns)
        select: str = f'SELECT {column_str} FROM {tablename} {conditions}'
        
        return select
    
    @staticmethod
    def delete(
        tablename: str,
        conditions: str = ''
    ) -> str:
        '''
        Get a SQL DELETE FROM statement.

        Parameters
        ----------
        tablename : str
            Name of the table from which to delete.
        conditions : str, optional
            Optional WHERE conditions. The default is ''.

        Returns
        -------
        delete : str
            A formatted SQL DELETE FROM statement.

        '''
        delete: str = f'DELETE FROM {tablename} {conditions}'
        
        return delete
    
    @staticmethod
    def update(
        tablename: str,
        set_cols: list[str],
        set_values: list[Any],
        conditions: str = ''
    ) -> str:
        '''
        Get a SQL UPDATE statement.

        Parameters
        ----------
        tablename : str
            Name of the table in which to update rows.
        set_cols : list[str]
            List of column names to update.
        set_values : list[Any]
            List of values with which to update columns. Paired with 
            set_cols such that set_cols[i] = set_values[i].
        conditions : str, optional
            A SQL-formatted string of conditions. The default is ''.

        Returns
        -------
        update : str
            A formatted SQL UPDATE statement.

        '''
        set_text: str = ''
        
        for i, _ in enumerate(set_cols):
            set_text += f"{set_cols[i]} = '{set_values[i]}'"
            
        update: str = f'UPDATE {tablename} SET {set_text} {conditions}'
        
        return update
    
    @staticmethod
    def where(
        cols: list[str],
        vals: list[Any],
        conjunction: str = 'AND'
    ) -> str:
        '''
        Construct WHERE condition from columns & values

        Parameters
        ----------
        cols : list[str]
            List of column names.
        vals : list[Any]
            List of values.
        conjunction : str, optional
            SQL keyword to use as conjunction between 
            clauses (e.g., AND, OR).

        Returns
        -------
        conditions : str
            A SQL-formatted WHERE condition.

        '''
        where: str = f"WHERE {cols[0]} = '{vals[0]}' "
        
        where += ' '.join([
            f"{conjunction} {cols[i]} = '{vals[i]}'" for i in range(
                1, len(cols)
            )
        ])
        
        return where


class Tabula:
    '''
    Defines columns & data types of indexia tables.
    
    '''
    @staticmethod
    def get_creator_table(
        genus: str,
        trait: str
    ) -> tuple[str, dict[str, str]]:
        '''
        Get name & columns of a creator (parent) table.

        Parameters
        ----------
        genus : str
            Name of the creator (parent) table.
        trait : str
            Name of the creator's text attribute.

        Returns
        -------
        creator_table : tuple[str, dict[str, str]]
            A tuple whose first entry is the name of the creator table, 
            & whose second is a dict of table columns & data types.

        '''
        creator_table: tuple[str, dict[str, str]] = (genus, {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL',
            trait: 'TEXT UNIQUE NOT NULL'
        })
        
        return creator_table
    
    @staticmethod
    def get_creature_table(
        creator: str,
        species: str,
        trait: str
    ) -> tuple[str, dict[str, str]]:
        '''
        Get name & columns of a creature (child) table.

        Parameters
        ----------
        creator : str
            Name of the creator (parent) table.
        species : str
            Name of the creature table.
        trait : str
            Name of the creature's text attribute.

        Returns
        -------
        creature_table : tuple[str, dict[str, str]]
            A tuple whose first entry is the name of the creature table,
            & whose second is a dict of table columns & data types.

        '''
        creature_table: tuple[str, dict[str, str]] = (species, {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            trait: 'TEXT NOT NULL',
            f'{creator}_id': 'INTEGER NOT NULL',
            f'FOREIGN KEY ({creator}_id)': Tabula.references(creator, 'id')
        })
        
        return creature_table
    
    @staticmethod
    def references(
        tablename: str, 
        on_column: str, 
        on_delete: str = 'CASCADE', 
        on_update: str = 'CASCADE'
    ) -> str:
        '''
        Generate SQL-formatted REFERENCES clause.

        Parameters
        ----------
        tablename : str
            Name of the referenced table.
        on_column : str
            Name of the referenced column.
        on_delete : str, optional
            Behavior of the child entity when the parent 
            entity is deleted. The default is 'CASCADE'.
        on_update : str, optional
            Behavior of the child entity when the parent 
            entity is updated. The default is 'CASCADE'.

        Returns
        -------
        references : str
            A SQL-formatted REFERENCES clause.

        '''
        references: str = f'REFERENCES {tablename}({on_column})'
        references = f'{references} ON DELETE {on_delete}'
        references = f'{references} ON UPDATE {on_update}'
        
        return references