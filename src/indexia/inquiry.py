'''
inquiry

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