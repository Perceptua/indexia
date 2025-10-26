'''
Defines tree & graph representations of indexia data.

'''
from indexia.indexia import Indexia
from pyvis.network import Network # type: ignore
from sqlite3 import Connection
from typing import Any
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
    def __init__(
        self,
        db: str
    ) -> None:
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
        self.db: str = db
    
    def upward(
        self,
        species: str,
        creature: pd.DataFrame
    ) -> list[tuple[str, pd.DataFrame]]:
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
        next_rung : list[tuple[str, pd.DataFrame]]
            list containing one tuple of the form (genus, creator),
            where genus is the name of the creator table & creator 
            is a single-row dataframe of creator entity data.

        '''
        with Indexia(self.db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            next_rung: list[tuple[str, pd.DataFrame]] = ix.get_creator(cnxn, species, creature)
             
        return next_rung
     
    def downward(
        self,
        genus: str,
        creator: pd.DataFrame
    ) -> list[tuple[str, pd.DataFrame]]:
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
        next_rung : list[tuple[str, pd.DataFrame]]
            list of tuples of the form (species, creature), 
            where species is the name of the creature table 
            & creature is a dataframe of creature entity data.

        '''
        with Indexia(self.db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            next_rung: list[tuple[str, pd.DataFrame]] = ix.get_creatures(cnxn, genus, creator)
            
        return next_rung
     
    def climb(
        self,
        kind: str,
        being: pd.DataFrame,
        direction: str
    ) -> list[tuple[str, pd.DataFrame]]:
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
            If direction is not either 'up' or 'down', raise 
            a ValueError.

        Returns
        -------
        next_rung : list[tuple[str, pd.DataFrame]]
            list of tuples of the form (kind, beings), where 
            kind is the name of a creator or creature table,
            & beings is a dataframe of creator or creature 
            entity data.

        '''
        if direction == 'up':
            next_rung: list[tuple[str, pd.DataFrame]] = self.upward(
                kind, being
            )
        elif direction == 'down':
            next_rung: list[tuple[str, pd.DataFrame]] = self.downward(
                kind, being
            )
        else:
            raise ValueError('climb direction must be "up" or "down".')
            
        return next_rung


class Dendron:
    '''
    Represent indexia data as an XML tree.
    
    '''
    def __init__(self, db: str) -> None:
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
        self.db: str = db
        self.trunk: ScalaNaturae = ScalaNaturae(self.db)
        
    def render_image(
        self,
        genus: str,
        creators: pd.DataFrame,
        root: et.Element = et.Element('root')
    ) -> et.ElementTree:
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
        for _, creator in creators.iterrows():
            attrs: dict[str, Any] = {c: creator[c] for c in creators.columns}
            
            next_rung: list[tuple[str, pd.DataFrame]] = self.trunk.downward(
                genus, pd.DataFrame(data=attrs, index=[0])
            )
            
            branch: et.Element[str] = et.SubElement(
                root, genus, attrib={a: str(attrs[a]) for a in attrs}
            )
            
            for species, creatures in next_rung:
                self.render_image(
                    species, creatures, root=branch
                )
        
        image: et.ElementTree[et.Element[str] | None] = et.ElementTree(root)
        
        return image
            
    def write_image(
        self,
        image: et.ElementTree,
        file_path: str = '',
        open_browser: bool = True
    ) -> str:
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
    def __init__(
        self,
        db: str,
        genus: str,
        creators: pd.DataFrame,
        max_depth: int = 10
    ) -> None:
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
        self.db: str = db
        self.genus: str = genus
        self.creators: pd.DataFrame = creators
        self.max_depth: int = max_depth
        self.spine = ScalaNaturae(self.db)
    
    def get_trait(
        self,
        species: str
    ) -> str:
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
            cnxn: Connection = ix.open_cnxn(ix.db)
            trait: str = ix.get_trait(cnxn, species)
        
        return trait
    
    def make_member(
        self,
        genus: str | None,
        creator: pd.DataFrame,
        species: str,
        creatures: pd.DataFrame
    ) -> pd.DataFrame:
        '''
        Creates a dataframe of indexia entity data.

        Parameters
        ----------
        genus : str | None
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
        creator_id: None | int = None if creator.empty else int(
            list(creator.id)[0]
        )
        
        trait: str = self.get_trait(species)
        member = pd.DataFrame()
        
        for _, creature in creatures.iterrows():
            member: pd.DataFrame = pd.concat([member, pd.DataFrame(data={
                'genus': [genus],
                'creator_id': [creator_id],
                'species': [species],
                'creature_id': [creature['id']],
                'trait': [trait],
                'expression': [creature[trait]]
            })], axis=0)
            
        return member
    
    def make_limbs(
        self,
        genus: str,
        creator: pd.DataFrame,
        depth: int
    ) -> list[pd.DataFrame]:
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
        limbs : list[pd.DataFrame]
            list of dataframes representing indexia entity 
            data.

        '''
        limbs: list[pd.DataFrame] = []
        
        if depth < self.max_depth:
            next_rung: list[tuple[str, pd.DataFrame]] = self.spine.downward(genus, creator)
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
    
    def assemble(
        self
    ) -> pd.DataFrame:
        '''
        Assemble the corpus of each of the creator entities.

        Returns
        -------
        corpus : pandas.DataFrame
            Dataframe representing all creatures of the 
            instance's creator entity, up to the distance 
            specified by max_depth.

        '''
        head: pd.DataFrame = self.make_member(
            None, pd.DataFrame(), self.genus, self.creators
        )
        
        limbs: list[pd.DataFrame] = []
        
        for i in range(self.creators.shape[0]):
            creator = self.creators.iloc[[i]]
            
            limbs += [pd.concat(self.make_limbs(
                self.genus, creator, 0
            ), axis=0)]
            
        body: pd.DataFrame = pd.concat(limbs, axis=0)
        corpus: pd.DataFrame = pd.concat([head, body], axis=0)
        corpus.index = pd.Index([i for i in range(corpus.shape[0])])
        
        return corpus
    
    def to_csv(
        self,
        corpus: pd.DataFrame,
        file_path: str,
        **kwargs: Any
    ) -> str:
        '''
        Save an assembled corpus dataframe to a CSV file.

        Parameters
        ----------
        corpus : pandas.DataFrame
            Dataframe representing indexia data, created by the 
            assemble method of this class.
        file_path : str
            Path of the CSV file to be created.
        **kwargs : Any
            Any keyword arguments accepted by pandas.DataFrame.to_csv.

        Returns
        -------
        file_path : str
            Path to the corpus CSV file.

        '''
        corpus.to_csv(file_path, **kwargs) # type: ignore
        
        return file_path

class Diktua:
    '''
    Represent indexia data as a network graph.
    
    '''
    def __init__(
        self,
        corpus: pd.DataFrame,
        as_nodes: str,
        as_edges: str,
        self_edges: bool = False
    ) -> None:
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
        self.corpus: pd.DataFrame = corpus
        self.as_nodes: str = as_nodes
        self.as_edges: str = as_edges
        self.self_edges: bool = self_edges
        self.make_undirected_graph()
        
    def get_graph_elements(
        self
    ) -> tuple[list[Any], list[tuple[Any, Any]]]:
        '''
        Get graph nodes & edges.

        Returns
        -------
        nodes : list[Any]
            list of graph nodes.
        edges : list[tuple[Any, Any]]
            list of tuples representing graph edges.

        '''
        sharing_nodes: list[Any] = list(
            self.corpus.groupby(by=self.as_edges).groups.values() # type: ignore
        )
        
        def get_nodes(index_list: list[Any]) -> list[Any]:
            return list(
                self.corpus.loc[index_list][self.as_nodes]
            )
        
        edge_set: set[tuple[Any, Any]] = set()
        
        for indices in sharing_nodes:
            node_edges: list[tuple[Any, Any]] = [tuple(sorted(c)) for c in list(
                itertools.combinations(get_nodes(indices), 2)
            )]
                        
            if not self.self_edges:
                node_edges = [e for e in node_edges if e[0] != e[1]]
                
            edge_set = edge_set.union(set(node_edges))
        
        nodes: list[Any] = list(set(e for edge in edge_set for e in edge))
        edges: list[tuple[Any, Any]]= list(edge_set)
                
        return nodes, edges
    
    def make_undirected_graph(
        self
    ) -> None:
        '''
        Create an undirected network graph from 
        the corpus attribute of the instance.

        Returns
        -------
        G : networkx.Graph
            And undirected network graph of 
            instance data.

        '''
        elements: tuple[list[Any], list[tuple[Any, Any]]] = self.get_graph_elements()
        nodes: list[Any] = elements[0]
        edges: list[tuple[Any, Any]] = elements[1]
        G: nx.Graph = nx.Graph() # type: ignore
        G.add_nodes_from(nodes) # type: ignore
        G.add_edges_from(edges) # type: ignore
        self.G: nx.Graph = G  # type: ignore
        
        return None
    
    def get_node_info(
        self
    ) -> tuple[dict[Any, int], dict[Any, str]]:
        '''
        Count node edges & assign titles.
        
        Edge counts are used to determine node size 
        when the graph is displayed; titles are shown when 
        hovering over nodes in the display.

        Returns
        -------
        node_edges : dict[Any, int]
            Keys are graph nodes; values are counts 
            of edges on each node.
        node_titles : dict[Any, str]
            Keys are graph nodes; values are string 
            titles assigned to nodes.

        '''
        node_edges: dict[Any, Any] = {}
        node_titles: dict[Any, Any] = {}

        for _, adjacencies in enumerate(self.G.adjacency()): # type: ignore
            node: Any = adjacencies[0] # type: ignore
            adj: dict[Any, Any] = adjacencies[1] # type: ignore
            num_edges: int = len(adj)
            node_edges[node] = num_edges
            node_titles[node] = f'({num_edges})'

        return node_edges, node_titles
    
    def get_node_sizes(
        self,
        node_edges: dict[Any, int],
        min_size: int,
        max_size: int
    ) -> dict[Any, int]:
        '''
        Calculate node size based on number of edges.
        
        Node sizes are scaled to the interval [min_size, max_size].

        Parameters
        ----------
        node_edges : dict[Any, int]
            dictionary of graph nodes & edge counts.
        min_size : int
            Minimum node size.
        max_size : int
            Maximum node size.

        Returns
        -------
        node_sizes : dict[Any, int]
            Keys are graph nodes; values are node sizes.

        '''
        max_edges: int = max(node_edges.values())
        offset: int = max_size - min_size
        node_sizes: dict[Any, int] = {}
            
        for n in node_edges:
            node_size: int = min_size + round(
                (offset * (node_edges[n] / max_edges))
            )
            
            node_sizes[n] = node_size
            
        return node_sizes
    
    def style_nodes(
        self,
        min_size: int = 7,
        max_size: int = 49
    ) -> None:
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
        node_info: tuple[dict[Any, int], dict[Any, str]] = self.get_node_info()
        node_edges: dict[Any, int]  = node_info[0]
        node_titles: dict[Any, str] = node_info[1]

        node_sizes: dict[Any, int] = self.get_node_sizes(
            node_edges, min_size, max_size
        )
        nx.set_node_attributes(self.G, node_sizes, 'size') # type: ignore
        nx.set_node_attributes(self.G, node_titles, 'title') # type: ignore
        
        return None

    def plot(
        self,
        plot_path: str | None = None,
        open_browser: bool = False
    ) -> tuple[Network, str | None]:
        """
        Create a plot of the instance's graph.

        Parameters
        ----------
        plot_path : str | None, optional
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
        plot_path : str | None
            If plot_path is set, returns the path of 
            the output HTML file. Otherwise None.

        """
        plot: Network = Network(select_menu=True, filter_menu=True)
        plot.from_nx(self.G) # type: ignore
        plot.show_buttons() # type: ignore
        
        if plot_path:
            plot.write_html(plot_path, open_browser=open_browser) # type: ignore
        
        return plot, plot_path
        
    def to_csv(
        self,
        file_path: str,
        **kwargs: Any
    ) -> str:
        """
        Save the edges of the instance's graph to a CSV file 
        with columns 'source' & 'target'.

        Parameters
        ----------
        file_path : str
            Path of the CSV file to be created.
        **kwargs : Any
            Any keyword arguments accepted by pandas.DataFrame.to_csv.

        Returns
        -------
        file_path : str
            Path to the output CSV file.

        """
        edges = pd.DataFrame(data={
            'source': [i[0] for i in self.G.edges], # type: ignore
            'target': [i[1] for i in self.G.edges] # type: ignore
        })
        
        edges.to_csv(file_path, **kwargs) # type: ignore
        
        return file_path
    