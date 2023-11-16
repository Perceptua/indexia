'''
Generate SQL for indexia database oprerations.

'''
class Inquiry:
    '''
    Generate SQL strings from dynamic inputs. 
    
    '''
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
        create = f'CREATE TABLE IF NOT EXISTS {tablename}'
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
        values = ','.join(
            '(' + ','.join(f"'{v[i]}'" for i,_ in enumerate(v)) + ')' 
            for v in values
        )
        
        columns = f" ({','.join(columns)})" if columns else ''
        insert = f'INSERT INTO {tablename}{columns}'
        insert = f'{insert} VALUES {values}'
        
        return insert
    
    def select(tablename, columns, conditions=''):
        '''
        GET a SQL SELECT statement.

        Parameters
        ----------
        tablename : str
            Name of the table from which to select values.
        columns : list(str)
            list of column names to select.
        conditions : str, optional
            A SQL-formatted string of conditions. The default is ''.

        Returns
        -------
        select : str
            A formatted SQL SELECT statement.

        '''
        columns = ','.join(columns)
        select = f'SELECT {columns} FROM {tablename} {conditions}'
        
        return select
    
    def delete(tablename, conditions=''):
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
        delete = f'DELETE FROM {tablename} {conditions}'
        
        return delete
    
    def update(tablename, set_cols, set_values, conditions=''):
        '''
        Get a SQL UPDATE statement.

        Parameters
        ----------
        tablename : str
            Name of the table in which to update rows.
        set_cols : list(str)
            List of column names to update.
        set_values : list(any)
            List of values with which to update columns. Paired with 
            set_cols such that set_cols[i] = set_values[i].
        conditions : str, optional
            A SQL-formatted string of conditions. The default is ''.

        Returns
        -------
        update : TYPE
            DESCRIPTION.

        '''
        set_text = ''
        
        for i, _ in enumerate(set_cols):
            set_text += f"{set_cols[i]} = '{set_values[i]}'"
            
        update = f'UPDATE {tablename} SET {set_text} {conditions}'
        
        return update
    
    def where(cols, vals, conjunction='AND'):
        '''
        Construct WHERE condition from columns & values

        Parameters
        ----------
        cols : list(str)
            List of column names.
        vals : list(any)
            List of values.
        conjunction : str, optional
            SQL keyword to use as conjunction between 
            clauses (e.g., AND, OR).

        Returns
        -------
        conditions : str
            A SQL-formatted WHERE condition.

        '''
        where = f"WHERE {cols[0]} = '{vals[0]}' "
        
        where += ' '.join([
            f"{conjunction} {cols[i]} = '{vals[i]}'" for i in range(1, len(cols))
        ])
        
        return where


class Tabula:
    '''
    Defines columns & data types of indexia tables.
    
    '''
    def get_creator_table(genus, trait):
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
        creator_table : tuple(str, dict)
            A tuple whose first entry is the name of the creator table, 
            & whose second is a dict of table columns & data types.

        '''
        creator_table = (genus, {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL',
            trait: 'TEXT UNIQUE NOT NULL'
        })
        
        return creator_table
    
    def get_creature_table(creator, species, trait):
        '''
        Get name & columns of a creature (child) table.

        Parameters
        ----------
        creator : str
            Name of the creator (parent) table.
        name : str
            Name of the creature table.
        attribute : str
            Name of the creature's text attribute.

        Returns
        -------
        creature_table : tuple(str, dict)
            A tuple whose first entry is the name of the creature table,
            & whose second is a dict of table columns & data types.

        '''
        creature_table = (species, {
            'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            trait: 'TEXT NOT NULL',
            f'{creator}_id': 'INTEGER NOT NULL',
            f'FOREIGN KEY ({creator}_id)': Tabula.references(creator, 'id')
        })
        
        return creature_table
    
    def references(
            tablename, on_column, on_delete='CASCADE', on_update='CASCADE'
        ):
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
        references = f'REFERENCES {tablename}({on_column})'
        references = f'{references} ON DELETE {on_delete}'
        references = f'{references} ON UPDATE {on_update}'
        
        return references