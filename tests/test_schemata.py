from datetime import datetime as dt, timedelta as td
from indexia.drone import Drone
from indexia.indexia import Indexia
from indexia.schemata import CorpusXML, Indexinet, Tabula
from pyvis.network import Network
import itertools
import os
import pandas as pd
import unittest as ut
import xml.etree.ElementTree as et


class TestCorpusXML(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pseudonym = 'test_scribe'
        cls.libronym = 'test_library'
        cls.created = dt.now().strftime('%Y-%m-%d-%H-%M-%S')
        cls.logonym = 'test_logonym'
        
        cls.test_db = os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/test.db'
        )
        
        cls.image_file = os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/corpus.xml'
        )
        
        cls.corpus = cls.makeObjects()
        cls.xml = CorpusXML(cls.corpus)
        
    @classmethod
    def makeObjects(cls):
        drone = Drone(cls.test_db)
        scribe = drone.ix.add_scribe(drone.cnxn, cls.pseudonym)
        library = drone.ix.add_library(drone.cnxn, scribe, cls.libronym)
        card = drone.ix.add_card(drone.cnxn, library, cls.created)
        drone.ix.add_logonym(drone.cnxn, card, cls.logonym)
        corpus = drone.get_corpus(scribe)
        drone.ix.close_all_cnxns()
        
        return corpus
            
    def testRenderImage(self):
        image = self.xml.render_image()
        self.assertIsInstance(image, et.ElementTree)
    
    def testDisplayImage(self):
        image = self.xml.render_image()
        self.xml.display_image(image, open_browser=False)
        self.assertFalse(os.path.isfile(self.image_file))
        
    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls.test_db)
        except:
            pass

class TestIndexinet(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/test.db'
        )
        
        cls.excel_name = 'indexinet'
        cls.pseudonym = 'test_scribe'
        cls.libronym = 'test_library'
        cls.choices = ['one', 'two', 'three']
        cls.cards, cls.logonyms = cls.makeTestObjects()
        cls.self_edges = False
        
        cls.indexinet = Indexinet(
            cls.logonyms, 
            as_nodes='logonym', 
            as_edges='card_id',
            self_edges=cls.self_edges
        )
    
    @classmethod
    def makeTestObjects(cls):
        cards, logonyms = pd.DataFrame(), pd.DataFrame()
        
        with Indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, cls.pseudonym)
            library = ix.add_library(cnxn, scribe, cls.libronym)
            
            for i, _ in enumerate(cls.choices):
                created = (
                    dt.now() + td(minutes=i)
                ).strftime('%Y-%m-%d-%H-%M') 
                
                card = ix.add_card(cnxn, library, created=created)
                
                [ix.add_logonym(
                    cnxn, card, cls.choices[x]
                ) for x, _ in enumerate(cls.choices)]
                
                logonyms = ix.get_logonyms(cnxn, card)
                cards = pd.concat([cards, card])
                logonyms = pd.concat([logonyms, logonyms])
        
        return cards, logonyms        
    
    @classmethod
    def get_expected_edges(cls, choices, self_edges):
        expected_edges = set()
        
        for choice in choices:
            node_edges = set(itertools.combinations(choices, 2))
            
            if self_edges:
                node_edges = node_edges.union({(choice, choice)})
            
            expected_edges = expected_edges.union(node_edges)
        
        expected_edges = [tuple(sorted(e)) for e in expected_edges]
        expected_edges = list(sorted(expected_edges))
        
        return expected_edges
    
    @classmethod
    def sort_edges(cls, edges):
        edges = list(sorted([tuple(sorted(e)) for e in edges]))
        
        return edges
    
    def testGetGraphElements(self):
        expected_nodes = list(sorted(self.choices))
        
        expected_edges = self.get_expected_edges(
            self.choices, self.self_edges
        )
        
        nodes, edges = self.indexinet.get_graph_elements()
        self.assertEqual(list(sorted(nodes)), expected_nodes)
        self.assertEqual(self.sort_edges(edges), expected_edges)
        
        expected_edges = self.get_expected_edges(
            self.choices, not self.self_edges
        )
        
        self.indexinet.self_edges = not self.self_edges
        nodes, edges = self.indexinet.get_graph_elements()
        self.assertEqual(self.sort_edges(edges), expected_edges)
        
        expected_edges = self.get_expected_edges(
            self.choices, self.self_edges
        )
        
        nodes, edges = self.indexinet.get_graph_elements()
        self.assertNotEqual(self.sort_edges(edges), expected_edges)
    
    def testMakeUndirectedGraph(self):
        expected_nodes = list(sorted(self.choices))
        
        expected_edges = self.get_expected_edges(
            self.choices, 
            self.self_edges
        )
        
        self.indexinet.self_edges = self.self_edges
        G = self.indexinet.make_undirected_graph()
        self.assertEqual(list(sorted(G.nodes)), expected_nodes)        
        self.assertEqual(self.sort_edges(G.edges), expected_edges)
        
    def testGetNodeInfo(self):
        node_connections, node_titles = self.indexinet.get_node_info()
        
        if self.self_edges:
            expected_connections = {len(self.choices)}
            expected_titles = {f'({len(self.choices)})'}
            
            self.assertEqual(
                set(node_connections.values()), 
                expected_connections
            )
            
            self.assertEqual(set(node_titles.values()), expected_titles)
        else:
            expected_connections = {len(self.choices) - 1}
            expected_titles = {f'({len(self.choices) - 1})'}
            
            self.assertEqual(
                set(node_connections.values()), 
                expected_connections
            )
            
            self.assertEqual(set(node_titles.values()), expected_titles)
            
    def testGetNodeSizes(self):
        min_size = 7
        max_size = 49
        
        if self.self_edges:
            max_connections = len(self.choices)
        else:
            max_connections = len(self.choices) - 1
        
        connections, _ = self.indexinet.get_node_info()
        
        node_sizes = self.indexinet.get_node_sizes(
            connections, min_size, max_size
        )
        
        expected_sizes = {max_size}
        self.assertEqual(set(node_sizes.values()), expected_sizes)
        
        connections[self.choices[0]] = 0
        
        node_sizes = self.indexinet.get_node_sizes(
            connections, min_size, max_size
        )
        
        expected_sizes = {min_size, max_size}
        self.assertEqual(set(node_sizes.values()), expected_sizes)
        
        connections[self.choices[1]] = 1
        
        node_sizes = self.indexinet.get_node_sizes(
            connections, min_size, max_size
        )
        
        mid_size = min_size + round(
            (max_size - min_size) * ( 1/ max_connections)
        )
        
        expected_sizes = {min_size, mid_size, max_size}
        self.assertEqual(set(node_sizes.values()), expected_sizes)
        
    def testStyleNodes(self):
        min_size = 7
        max_size = 49
        
        if self.self_edges:
            max_connections = len(self.choices)
        else:
            max_connections = len(self.choices) - 1
            
        expected_title = f'({max_connections})'
        
        expected_result = {choice: {
            'size': max_size, 
            'title': expected_title
        } for choice in self.choices}
        
        self.indexinet.style_nodes(min_size=min_size, max_size=max_size)
        result = self.indexinet.G.nodes.data()
        self.assertDictEqual(dict(result), expected_result)
        
    def testPlot(self):
        plot, file = self.indexinet.plot('indexinet', show=False)
        self.assertIsInstance(plot, Network)
        self.assertIsNone(file)
    
    def testToExcel(self):
        excel_path = self.indexinet.to_excel(self.excel_name)
        self.assertTrue(os.path.isfile(excel_path))
    
    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls.test_db)
            os.remove(cls.excel_file)
        except:
            pass


class TestTabula(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tablename = 'scribes'
        cls.on_column = 'pseudonym'
        cls.data_type = 'text'
        cls.tabula = Tabula()
        
    def testCheck(self):
        check = self.tabula.check(self.on_column, self.data_type)
        expected = f"CHECK(typeof({self.on_column}) = '{self.data_type}')"
        self.assertEqual(check, expected)
    
    def testReferences(self):
        references = self.tabula.references(
            self.tablename, 
            self.on_column
        )
        
        expected = ' '.join([
            f'REFERENCES {self.tablename}({self.on_column})',
            'ON DELETE CASCADE ON UPDATE CASCADE'
        ])
        
        self.assertEqual(references, expected)


if __name__ == '__main__':
    ut.main()