'''
drone

Defines shorthand for common indexia operations.

'''
from indexia.indexia import Indexia
from indexia.schemata import CorpusXML, Indexinet
import os

class Drone:
    '''
    Convenience class for manipulating & displaying indexia 
    objects.
    
    '''         
    def __init__(self, db=None):
        '''
        Create a Drone instance for a database file. Set 
        the attribute ix to an Indexia instance & open a 
        connection to the database file.

        Parameters
        ----------
        db : str, optional
            Path to a database file. If None, the Indexia 
            default will be used. The default is None.

        Returns
        -------
        None.

        '''
        self.db = db
        self.ix = Indexia(db)
        self.cnxn = self.ix.open_cnxn(self.ix.db)
    
    def get_corpus(self, scribe):
        '''
        Get all logonyms on all cards in all libraries 
        for a given scribe.

        Parameters
        ----------
        scribe : pandas.DataFrame
            A single-row dataframe of scribe data.

        Returns
        -------
        corpus : pandas.DataFrame
            Dataframe containing all logonyms on all cards 
            in all libraries for the given scribe.

        '''
        (scribe_id, pseudonym) = scribe.loc[0]
        
        sql_file = os.path.join(
            os.path.abspath(__file__),
            '..',
            'resources/queries/corpus.sql'
        )
        
        sql = open(sql_file).read().format(scribe_id=scribe_id)
        
        corpus = self.ix.get_df(
            self.cnxn, sql, 
            expected_columns=[
                'library_id', 'libronym',
                'card_id', 'created',
                'logonym_id', 'logonym'
            ],
            raise_errors=True
        )
        
        return corpus
    
    def get_indexinet(self, data, as_nodes, as_edges, self_edges=False):
        '''
        Create a network graph from indexia data.

        Parameters
        ----------
        data : pandas.DataFrame
            Dataframe of objects to graph.
        as_nodes : str
            Name of dataframe column representing nodes.
        as_edges : str
            Name of dataframe column representing edges.
        self_edges : bool, optional
            If True, self edges are allowed in the graph. 
            The default is False.

        Returns
        -------
        indexinet : Indexinet
            Indexinet network graph instance.

        '''
        indexinet = Indexinet(data, as_nodes, as_edges, self_edges=self_edges)
        
        return indexinet
    
    def render_indexinet(self, indexinet, plot_name, style=True):
        '''
        Create & show a plot of the network graph.

        Parameters
        ----------
        indexinet : Indexinet
            Indexinet network graph instance to be plotted.
        plot_name : str
            Name for the HTML plot file.
        style : bool, optional
            If True, Indexinet node styles are applied to the 
            nodes of the graph. The default is True.

        Returns
        -------
        plot_file : str
            Path to the output HTML file.

        '''
        if style:
            indexinet.style_nodes()
        
        plot, plot_file = indexinet.plot(plot_name, show=True)
        
        return plot_file
    
    def render_corpus_xml(self, corpus):
        '''
        Render & display corpus data for a given scribe 
        as an XML tree.

        Parameters
        ----------
        corpus : pandas.DataFrame
            Dataframe of corpus data.

        Returns
        -------
        image : xml.etree.ElementTree.ElementTree
            An XML tree representing the corpus.

        '''
        corpus_xml = CorpusXML(corpus)
        image = corpus_xml.render_image()
        corpus_xml.display_image(image)
        
        return image
    
    def get_table(self, tablename, order_by=None):
        '''
        Get all records from the specified table.

        Parameters
        ----------
        tablename : str
            Name of the table to retrieve.
        order_by : list(str), optional
            Supply a list of column names to order results. 
            To sort in descending order, format list entries 
            as '<column_name> DESC'. The default is None.

        Returns
        -------
        table : pandas.DataFrame
            Dataframe of table data.

        '''
        sql = f'SELECT * FROM {tablename}'
        
        if order_by:
            order_by = ', '.join(order_by)
            sql = f'{sql} ORDER BY {order_by}'
        
        table = self.ix.get_df(self.cnxn, sql)
        
        return table
    
    def reindex_table(self, tablename, table):
        '''
        Reset the primary keys of a table. If the table 
        contains N rows, the rows will be labeled with 
        integers 1 to N.
        
        The order of the rows is determined by the table 
        argument. Rows are not reordered by this method.

        Parameters
        ----------
        tablename : str
            Name of the table to reindex.
        table : pandas.DataFrame
            Dataframe containing table data. The order 
            of the dataframe rows determines the order 
            of the reindexed table rows.

        Returns
        -------
        result : pandas.DataFrame
            The reindexed tables.

        '''
        old_ids = list(table.id.values)
        swap_ids = [i + table.shape[0] for i in old_ids]
        new_ids = list(range(1, table.shape[0] + 1))
        
        for i in range(table.shape[0]):
            self.ix.update(
                self.cnxn, tablename, 
                ['id'], [swap_ids[i]], 
                ['id'], [old_ids[i]]
            )
            
        for i in range(table.shape[0]):
            self.ix.update(
                self.cnxn, tablename, 
                ['id'], [new_ids[i]], 
                ['id'], [swap_ids[i]]
            )
        
        self.ix.update(
            self.cnxn, 'SQLITE_SEQUENCE', 
            ['seq'], [table.shape[0]], 
            ['name'], [tablename]
        )
        
        result = self.get_table(tablename)
        
        return result
