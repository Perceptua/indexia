'''
Defines tree & graph representations of indexia data.

'''
from indexia.indexia import Indexia
from pyvis.network import Network
import itertools
import networkx as nx
import os
import pandas as pd
import time
import webbrowser
import xml.etree.ElementTree as et


class ScalaNaturae:
    '''
    Ascend & descend the hierarchy of indexia data.
    
    '''
    def __init__(self, db):
        '''
        Creates a ScalaNaturae instance.

        Parameters
        ----------
        db : str
            Path to the indexia database file.

        Returns
        -------
        None.

        '''
        self.db = db
    
    def upward(self, species, creature):
        '''
        Climb up one rung.

        Parameters
        ----------
        species : str
            Name of the starting creature table.
        creature : pandas.DataFrame
            A single-row dataframe of creature entity data.

        Returns
        -------
        next_rung : list(tuple)
            List containing one tuple of the form (genus, creator),
            where genus is the name of the creator table & creator 
            is a single-row dataframe of creator entity data.

        '''
        with Indexia(self.db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            next_rung = ix.get_creator(cnxn, species, creature)
             
        return next_rung
     
    def downward(self, genus, creator):
        '''
        Climb down one rung.

        Parameters
        ----------
        genus : str
            Name of the starting creator table.
        creator : pandas.DataFrame
            A single-row dataframe of creator entity data.

        Returns
        -------
        next_rung : list(tuple)
            List of tuples of the form (species, creature), 
            where species is the name of the creature table 
            & creature is a dataframe of creature entity data.

        '''
        with Indexia(self.db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            next_rung = ix.get_creatures(cnxn, genus, creator)
            
        return next_rung
     
    def climb(self, kind, being, direction):
        '''
        Climb one rung in either direction (up or down).

        Parameters
        ----------
        kind : str
            Name of the starting table.
        being : pandas.DataFrame
            Dataframe of creator or creature entities. If the 
            dataframe contains more than one row, only results 
            for the first row will be returned.
        direction : str
            Direction to climb. Must be either 'up' or 'down'.

        Raises
        ------
        ValueError
            If direction is not either 'up' or 'down', rasise 
            a ValueError.

        Returns
        -------
        next_rung : list(tuple)
            List of tuples of the form (kind, beings), where 
            kind is the name of a creator or creature table,
            & beings is a dataframe of creator or creature 
            entity data.

        '''
        if direction == 'up':
            next_rung = self.upward(kind, being)
        elif direction == 'down':
            next_rung = self.downward(kind, being)
        else:
            raise ValueError('climb direction must be "up" or "down".')
            
        return next_rung


class Dendron:
    '''
    Represent indexia data as an XML tree.
    
    '''
    def __init__(self, db):
        '''
        Creates a Dendron instance.
        
        Sets the trunk attribute to a ScalaNaturae instance 
        for ascending & descending the hierarchy of indexia 
        data.

        Parameters
        ----------
        db : str
            Path to the indexia database.

        Returns
        -------
        None.

        '''
        self.db = db
        self.trunk = ScalaNaturae(self.db)
        
    def render_image(self, genus, creators, root=et.Element('root')):
        '''
        Render the XML tree.

        Parameters
        ----------
        genus : str
            Name of the top-level table.
        creators : pandas.DataFrame
            One or more rows of the top-level table to 
            render as XML.
        root : xml.etree.ElementTree.Element, optional
            Root element of the XML tree, used in iterative 
            calls to this method. It is not typically 
            necessary to supply this argument. The default 
            is xml.etree.ElementTree.Element('root').

        Returns
        -------
        image : xml.etree.ElementTree.ElementTree
            An XML element tree of indexia data.

        '''
        for i, creator in creators.iterrows():
            attrs = {c: creator[c] for c in creators.columns}
            
            next_rung = self.trunk.downward(
                genus, pd.DataFrame(data=attrs, index=[0])
            )
            
            branch = et.SubElement(
                root, genus, attrib={a: str(attrs[a]) for a in attrs}
            )
            
            for species, creatures in next_rung:
                self.render_image(
                    species, creatures, root=branch
                )
        
        image = et.ElementTree(root)
        
        return image
            
    def write_image(self, image, file_path=None, open_browser=True):
        '''
        Write the XML image of the Dendron instance to 
        an XML file, & optionally open in the browser.

        Parameters
        ----------
        image : xml.etree.ElementTree.ElementTree
            Image of the current Dendron instance as an 
            XML tree.
        file_path : str, optional
            Path where the XML file will be created. If 
            None, the default (dendron.xml) is used. The 
            default is None.
        open_browser : bool, optional
            If True, open the XML file in the default browser. 
            The default is True.

        Returns
        -------
        file_path : str
            Absolute path to the XML image file.

        '''
        file_path = file_path if file_path else 'dendron.xml'
        
        if os.path.isfile(file_path):
            os.remove(file_path)
        
        image.write(file_path)
        
        if open_browser:
            webbrowser.open(f'file://{os.path.abspath(file_path)}')
            time.sleep(2)
            
        return file_path
    

class Corpus:
    '''
    Represent indexia data as a dataframe.
    
    '''
    def __init__(self, db, genus, creators, max_depth=10):
        '''
        Creates a Corpus instance for the given creator data.
        
        Sets the spine attribute to a ScalaNaturae instance 
        for descending the hierarchy of creator data.

        Parameters
        ----------
        db : str
            Path to the indexia database file.
        genus : str
            Name of the creator (parent) table.
        creators : pandas.DataFrame
            Dataframe of creator entity data.
        max_depth : int, optional
            Maximum number of levels to descend when assembling 
            the corpus. The default is 10.

        Returns
        -------
        None.

        '''
        self.db = db
        self.genus = genus
        self.creators = creators
        self.max_depth = max_depth
        self.spine = ScalaNaturae(self.db)
    
    def get_trait(self, species):
        '''
        Gets the trait (attribute) column of the given 
        species.

        Parameters
        ----------
        species : str
            Name of the creature (child) table.

        Returns
        -------
        trait : str
            Name of the trait column.

        '''
        with Indexia(self.db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            trait = ix.get_trait(cnxn, species)
        
        return trait
    
    def make_member(self, genus, creator, species, creatures):
        '''
        Creates a dataframe of indexia entity data.

        Parameters
        ----------
        genus : str
            Name of the creator (parent) table.
        creator : pandas.DataFrame
            Single-row dataframe of creator entity data.
        species : str
            Name of the creature (child) table.
        creatures : pandas.DataFrame
            Dataframe of creature entity data.

        Returns
        -------
        member : pandas.DataFrame
            Dataframe describing creature entities, including 
            creator information.

        '''
        creator_id = None if creator.empty else creator.id.values[0]
        trait = self.get_trait(species)
        member = pd.DataFrame()
        
        for i, creature in creatures.iterrows():
            member = pd.concat([member, pd.DataFrame(data={
                'genus': [genus],
                'creator_id': [creator_id],
                'species': [species],
                'creature_id': [creature['id']],
                'trait': [trait],
                'expression': [creature[trait]]
            })], axis=0)
            
        return member
    
    def make_limbs(self, genus, creator, depth):
        '''
        Moves down the spine to create lists of dataframes 
        representing indexia entity data.

        Parameters
        ----------
        genus : str
            Name of the creator (parent) table.
        creator : pandas.DataFrame
            Single-row dataframe of creator entity data.
        depth : int
            Current level in the corpus rendering process. 
            Compared with max_depth to determine whether 
            to proceed.

        Returns
        -------
        limbs : list(pandas.DataFrame)
            List of dataframes representing indexia entity 
            data.

        '''
        limbs = []
        
        if depth < self.max_depth:
            next_rung = self.spine.downward(genus, creator)
        else:
            next_rung = []        
        
        for species, creatures in next_rung:
            limbs += [self.make_member(
                genus, creator, species, creatures
            )]
            
            for i in range(creatures.shape[0]):
                limbs += self.make_limbs(
                    species, creatures.iloc[[i]], depth + 1
                )
        
        return limbs
    
    def assemble(self):
        '''
        Assemble the corpus of each of the creator entities.

        Returns
        -------
        corpus : pandas.DataFrame
            Dataframe representing all creatures of the 
            instance's creator entity, up to the distance 
            specified by max_depth.

        '''
        head = self.make_member(
            None, pd.DataFrame(), self.genus, self.creators
        )
        
        body = []
        
        for i in range(self.creators.shape[0]):
            creator = self.creators.iloc[[i]]
            
            body += [pd.concat(self.make_limbs(
                self.genus, creator, 0
            ), axis=0)]
            
        body = pd.concat(body, axis=0)
        corpus = pd.concat([head, body], axis=0)
        corpus.index = [i for i in range(corpus.shape[0])]
        
        return corpus
    
    def to_csv(self, corpus, file_path, **kwargs):
        '''
        Save an assembled corpus dataframe to a CSV file.

        Parameters
        ----------
        corpus : pandas.DataFrame
            Dataframe representing indexia data, created by the 
            assemble method of this class.
        file_path : str
            Path of the CSV file to be created.
        **kwargs : any
            Any keyword arguments accepted by pandas.DataFrame.to_csv.

        Returns
        -------
        file_path : str
            Path to the corpus CSV file.

        '''
        corpus.to_csv(file_path, **kwargs)
        
        return file_path

class Diktua:
    '''
    Represent indexia data as a network graph.
    
    '''
    def __init__(self, corpus, as_nodes, as_edges, self_edges=False):
        '''
        Creates an Indexinet instance.

        Parameters
        ----------
        corpus : pandas.DataFrame
            Dataframe of indexia data to represent as 
            a network graph.
        as_nodes : str
            Name of the creator or creature attribute 
            to treat as graph nodes.
        as_edges : str
            Name of the creator or creature attribute 
            to treat as graph edges.
        self_edges : bool, optional
            Whether to allow self-edges in the graph. 
            The default is False.

        Returns
        -------
        None.

        '''
        self.corpus = corpus
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
            self.corpus.groupby(self.as_edges).groups.values()
        )
        
        get_nodes = lambda index_list: list(
            self.corpus.loc[index_list][self.as_nodes]
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
        the corpus attribute of the instance.

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

    def plot(self, plot_path=None, open_browser=False):
        '''
        Create a plot of the instance's graph.

        Parameters
        ----------
        plot_path : str or None, optional
            If supplied, plot will be written to an 
            HTML file at plot_path. The default is 
            None.
        open_browser : bool, optional
            Whether to open the plot in the browser. 
            The default is False.

        Returns
        -------
        plot : pyvis.network.Network
            A plot of the instance's network graph.
        plot_path : str or None
            If plot_path is set, returns the path of 
            the output HTML file. Otherwise None.

        '''
        plot = Network(select_menu=True, filter_menu=True)
        plot.from_nx(self.G)
        plot.show_buttons()
        
        if plot_path:
            plot.write_html(plot_path, open_browser=open_browser)
        
        return plot, plot_path
        
    def to_csv(self, file_path, **kwargs):
        '''
        Save the edges of the instance's graph to a CSV file 
        with columns 'source' & 'target'.

        Parameters
        ----------
        file_path : str
            Path of the CSV file to be created.
        **kwargs : any
            Any keyword arguments accepted by pandas.DataFrame.to_csv.

        Returns
        -------
        file_path : str
            Path to the output CSV file.

        '''
        edges = pd.DataFrame(data={
            'source': [e[0] for e in self.G.edges],
            'target': [e[1] for e in self.G.edges]
        })
        
        edges.to_csv(file_path, **kwargs)
        
        return file_path
    