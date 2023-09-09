'''
schemata

Defines XML, network graph, & tabular representations of indexia data.

'''
from pyvis.network import Network
import itertools
import networkx as nx
import os
import pandas as pd
import time
import webbrowser
import xml.etree.ElementTree as et


class CorpusXML:
    '''
    Renders & displays indexia corpora as XML.
    
    Attributes
    ----------
    corpus : pandas.DataFrame
        Dataframe representing all cards created by a scribe.
    
    '''
    def __init__(self, corpus):
        '''
        Create a CorpusXML instance.

        Parameters
        ----------
        corpus : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        '''
        self.corpus = corpus
        
    def render_image(self):
        '''
        Create an XML image of the corpus.

        Returns
        -------
        image : xml.etree.ElementTree.ElementTree
            An XML tree representing the corpus.

        '''
        root = et.Element('root')
        libraries = {}
        cards = {}
        logonyms = {}
        
        for i, row in self.corpus.iterrows():
            if row['library_id'] not in libraries.keys():
                e = et.SubElement(
                    root, 
                    'library', 
                    id=str(row['library_id']),
                    libronym=str(row['libronym'])
                )
                
                libraries[row['library_id']] = e
            
            if row['card_id'] not in cards.keys():
                e = et.SubElement(
                    libraries[row['library_id']], 
                    'card',
                    id=str(row['card_id']),
                    created=str(row['created'])
                )
                
                cards[row['card_id']] = e
            
            if row['logonym_id'] not in logonyms.keys():
                e = et.SubElement(
                    cards[row['card_id']], 
                    'logonym', 
                    id=str(row['logonym_id']),
                    logonym=str(row['logonym'])
                )
                
                logonyms[row['logonym_id']] = e
        
        image = et.ElementTree(root)
        
        return image
    
    def display_image(self, image, open_browser=True):
        '''
        Display the corpus image in a browser window.

        Parameters
        ----------
        image : xml.etree.ElementTree
            An XML tree representing the corpus.
        open_browser : bool, optional
            Optional flag for testing. If True, the 
            image will be opened in a browser window. 
            The default is True.

        Returns
        -------
        None.

        '''
        file_path = os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/corpus.xml'
        )
        
        image.write(file_path)
        
        if open_browser:
            webbrowser.open(f'file://{file_path}')
            time.sleep(3)
            
        os.remove(file_path)


class Indexinet:
    '''
    Create network graphs of indexia data.
    
    '''
    def __init__(self, df, as_nodes, as_edges, self_edges=False):
        '''
        Create Indexinet instance & generate undirected 
        network graph from input data.

        Parameters
        ----------
        df : pandas.DataFrame
            Dataframe containing data to be graphed.
        as_nodes : str
            Name of dataframe column to treat as 
            graph nodes.
        as_edges : str
            Name of dataframe column to treat as 
            graph edges.
        self_edges : bool, optional
            Whether to allow self-edges in the graph. 
            The default is False.

        Returns
        -------
        None.

        '''
        self.df = df
        self.as_nodes = as_nodes
        self.as_edges = as_edges
        self.self_edges = self_edges
        self.G = self.make_undirected_graph()
        
    def get_graph_elements(self):
        '''
        Get graph nodes & edges.

        Returns
        -------
        nodes : list
            List of graph nodes.
        edges : list
            List of tuples representing graph edges.

        '''
        sharing_nodes = list(
            self.df.groupby(self.as_edges).groups.values()
        )
        
        get_nodes = lambda index_list: list(
            self.df.iloc[index_list][self.as_nodes]
        )
        
        edges = set()
        
        for indices in sharing_nodes:
            node_edges = [tuple(sorted(c)) for c in list(
                itertools.combinations(get_nodes(indices), 2)
            )]
                        
            if not self.self_edges:
                node_edges = [e for e in node_edges if e[0] != e[1]]
                
            edges = edges.union(set(node_edges))
        
        nodes = list(set(e for edge in edges for e in edge))
        edges = list(edges)
                
        return nodes, edges
    
    def make_undirected_graph(self):
        '''
        Create an undirected network graph from 
        the df attribute of the instance.

        Returns
        -------
        G : networkx.Graph
            And undirected network graph of 
            instance data.

        '''
        nodes, edges = self.get_graph_elements()
        G = nx.Graph()
        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        
        return G
    
    def get_node_info(self):
        '''
        Count node edges & assign titles.
        
        Edge counts are used to determine node size 
        when the graph is displayed; titles are shown when 
        hovering over nodes in the display.

        Returns
        -------
        node_edges : dict
            Keys are graph nodes; values are counts 
            of edges on each node.
        node_titles : dict
            Keys are graph nodes; values are string 
            titles assigned to nodes.

        '''
        node_edges = {}
        node_titles = {}

        for _, adjacencies in enumerate(self.G.adjacency()):
            node, adj = adjacencies
            num_edges = len(adj)
            node_edges[node] = num_edges
            node_titles[node] = f'({num_edges})'

        return node_edges, node_titles
    
    def get_node_sizes(self, node_edges, min_size, max_size):
        '''
        Calculate node size based on number of edges.
        
        Node sizes are scaled to the interval [min_size, max_size].

        Parameters
        ----------
        node_edges : dict
            Dictionary of graph nodes & edge counts.
        min_size : int
            Minimum node size.
        max_size : int
            Maximum node size.

        Returns
        -------
        node_sizes : dict
            Keys are graph nodes; values are node sizes.

        '''
        max_edges = max(node_edges.values())
        offset = max_size - min_size
        node_sizes = {}
            
        for n in node_edges:
            node_size = min_size + round(
                (offset * (node_edges[n] / max_edges))
            )
            
            node_sizes[n] = node_size
            
        return node_sizes
    
    def style_nodes(self, min_size=7, max_size=49):
        '''
        Set size & title attributes of graph nodes.

        Parameters
        ----------
        min_size : int, optional
            Minimum node size. The default is 7.
        max_size : int, optional
            Maximum node size. The default is 49.

        Returns
        -------
        networkx.Graph
            Network graph with node attributes set.

        '''
        node_edges, node_titles = self.get_node_info()
        node_sizes = self.get_node_sizes(node_edges, min_size, max_size)
        nx.set_node_attributes(self.G, node_sizes, 'size')
        nx.set_node_attributes(self.G, node_titles, 'title')
        
        return self.G

    def plot(self, plot_name, show=True):
        '''
        Create a plot of the instance's graph.

        Parameters
        ----------
        plot_name : str
            If show is True, plot_name is used as 
            the name of the output HTML file.
        show : bool, optional
            If True, the plot will be written to an 
            HTML file & opened in a browser window. 
            The default is True.

        Returns
        -------
        plot : pyvis.network.Network
            A plot of the instance's network graph.
        plot_file : str or None
            If show is True, returns the path of the 
            output HTML file. Otherwise None.

        '''
        plot = Network(select_menu=True, filter_menu=True)
        plot.from_nx(self.G)
        plot.show_buttons()
        plot_file = None
        
        if show:
            plot_file = os.path.join(
                os.path.abspath(__file__),
                '..', 'data', 'plots',
                f'{plot_name}.html'
            )
            
            plot.write_html(plot_file, open_browser=True)
        
        return plot, plot_file
        
    def to_excel(self, file_name):
        '''
        Save the edges of the instance's graph to 
        an excel file with columns 'source' & 'target'.

        Parameters
        ----------
        file_name : str
            Name of the output .xlsx file.

        Returns
        -------
        file_path : Path to the output
            DESCRIPTION.

        '''
        df = pd.DataFrame(data={
            'source': [e[0] for e in self.G.edges],
            'target': [e[1] for e in self.G.edges]
        })
        
        file_path = os.path.join(
            os.path.abspath(__file__),
            '..', 'data', f'{file_name}.xlsx'
        )
        
        df.to_excel(file_path, index=False)
        
        return file_path


class Tabula:
    '''
    Defines columns & data types of indexia tables.
    
    Attributes
    ----------
    columns : dict(str)
        A dictionary describing indexia tables. Keys 
        are table names & values are a dictionary of 
        columns & data types.
    
    '''
    def __init__(self):
        '''
        Define indexia tables, columns, & data types.

        Returns
        -------
        None.

        '''
        self.columns = {
            'scribes': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'pseudonym': ' '.join([
                    'TEXT UNIQUE NOT NULL',
                    self.check('pseudonym', 'text')
                ]),
            },
            'libraries': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'libronym': ' '.join([
                    'TEXT UNIQUE NOT NULL',
                    self.check('libronym', 'text')
                ]),
                'scribe_id': ' '.join([
                    'INTEGER NOT NULL', 
                    self.check('scribe_id', 'integer')
                ]),
                'FOREIGN KEY (scribe_id)': self.references('scribes', 'id'),
            },
            'cards': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'created': ' '.join([
                    'TEXT UNIQUE NOT NULL',
                    self.check('created', 'text')
                ]),
                'library_id' : ' '.join([
                    'INTEGER NOT NULL',
                    self.check('library_id', 'integer')
                ]),
                'FOREIGN KEY (library_id)': self.references('libraries', 'id'),
            },
            'logonyms': {
                'id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
                'logonym': ' '.join([
                    'TEXT NOT NULL',
                    self.check('logonym', 'text')
                ]),
                'card_id': ' '.join([
                    'INTEGER NOT NULL',
                    self.check('card_id', 'integer')
                ]),
                'FOREIGN KEY (card_id)': self.references('cards', 'id'),
            },
        }
        
    def check(self, column_name, data_type):
        '''
        Generate a SQLite CHECK constraint.

        Parameters
        ----------
        column_name : str
            Name of the column to constrain.
        data_type : str
            Data type contraint to enforce on the column.

        Returns
        -------
        check : str
            CHECK constraint ensuring entries in column_name 
            are of type data_type.

        '''
        check = f"CHECK(typeof({column_name}) = '{data_type}')"
        
        return check
    
    def references(
            self, tablename, on_column, 
            on_delete='CASCADE', on_update='CASCADE'
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