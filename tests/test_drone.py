from indexia.drone import Drone
from xml.etree import ElementTree as et
import itertools
import os
import pandas as pd
import unittest as ut


class TestDrone(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = os.path.join(
            os.path.abspath(__file__),
            '..',
            'data/test.db'
        )
        
        cls.drone = Drone(cls.test_db)
        
    @classmethod
    def makeTestObjects(cls, num_cards, logonyms_per_card):
        cls.scribe = cls.drone.ix.add_scribe(cls.drone.cnxn, 'test')
        
        cls.library = cls.drone.ix.add_library(
            cls.drone.cnxn, cls.scribe, 'test'
        )
        
        cls.cards = []
        cls.logonyms = []
        
        for i in range(num_cards):
            card = cls.drone.ix.add_card(
                cls.drone.cnxn, cls.library, f'card_{i}'
            )
            
            cls.cards += [card]
            
            for j in range(logonyms_per_card):
                cls.logonyms += [cls.drone.ix.add_logonym(
                    cls.drone.cnxn, card, f'logonym_{j}'
                )]
    
    def testGetCorpus(self):
        num_cards, logonyms_per_card = 3, 3
        self.makeTestObjects(num_cards, logonyms_per_card)
        corpus = self.drone.get_corpus(self.scribe)
        expected_rows = num_cards * logonyms_per_card
        self.assertEqual(corpus.shape[0], expected_rows)
    
    def testGetIndexinet(self):
        num_cards, logonyms_per_card = 3, 3
        self.makeTestObjects(num_cards, logonyms_per_card)
        corpus = self.drone.get_corpus(self.scribe)
        
        indexinet = self.drone.get_indexinet(
            corpus, as_nodes='created', as_edges='logonym'
        )
        
        expected_nodes = set(c.loc[0, 'created'] for c in self.cards)
        nodes = set(indexinet.G.nodes)
        self.assertEqual(expected_nodes, nodes)
        
        expected_edges = set(itertools.combinations(expected_nodes, 2))
        edges = set(indexinet.G.edges)
        self.assertEqual(expected_edges, edges)
    
    def testRenderIndexinet(self):
        num_cards, logonyms_per_card = 3, 3
        self.makeTestObjects(num_cards, logonyms_per_card)
        corpus = self.drone.get_corpus(self.scribe)
        
        indexinet = self.drone.get_indexinet(
            corpus, as_nodes='created', as_edges='logonym'
        )
        
        expected_plot_file = 'data\\plots\\test_plot.html'
        plot_file = self.drone.render_indexinet(indexinet, 'test_plot')
        self.assertTrue(plot_file.endswith(expected_plot_file))
    
    def testRenderCorpusXML(self):
        num_cards, logonyms_per_card = 3, 3
        self.makeTestObjects(num_cards, logonyms_per_card)
        corpus = self.drone.get_corpus(self.scribe)
        image = self.drone.render_corpus_xml(corpus)
        self.assertIsInstance(image, et.ElementTree)
    
    def testGetTable(self):
        num_cards, logonyms_per_card = 3, 3
        self.makeTestObjects(num_cards, logonyms_per_card)
        cards_unordered = self.drone.get_table('cards')
        
        expected_cards_unordered = pd.DataFrame(data={
            'id': [1, 2, 3],
            'created': ['card_0', 'card_1', 'card_2'],
            'library_id': [1, 1, 1]
        })
        
        pd.testing.assert_frame_equal(
            expected_cards_unordered, cards_unordered
        )
        
        cards_ordered = self.drone.get_table(
            'cards', order_by=['created DESC']
        )
        
        expected_cards_ordered = pd.DataFrame(data={
            'id': [3, 2, 1],
            'created': ['card_2', 'card_1', 'card_0'],
            'library_id': [1, 1, 1]
        })
                
        pd.testing.assert_frame_equal(
            expected_cards_ordered, cards_ordered    
        )
    
    def testReindexTable(self):
        num_cards, logonyms_per_card = 3, 3
        self.makeTestObjects(num_cards, logonyms_per_card)
        self.drone.ix.add_card(self.drone.cnxn, self.library, 'aaa')
        cards_original = self.drone.get_table('cards', order_by=['created'])
        self.assertEqual(list(cards_original.id), [4, 1, 2, 3])
        
        cards_reindexed = self.drone.reindex_table('cards', cards_original)
        self.assertEqual(list(cards_reindexed.id), [1, 2, 3, 4])
    
    @classmethod
    def tearDownClass(cls):
        cls.drone.ix.close_all_cnxns()
        
        try:
            os.remove(cls.test_db)
        except:
            pass


if __name__ == '__main__':
    ut.main()