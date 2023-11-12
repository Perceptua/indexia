from indexia.eidola import Maker
from indexia.indexia import Indexia
from indexia.schemata import Corpus, Dendron, Diktua, ScalaNaturae
from pyvis.network import Network
import itertools
import os
import pandas as pd
import unittest as ut
import xml.etree.ElementTree as et


class TestScalaNaturae(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = 'tests/data/test_schemata.db'
        cls.ladder = ScalaNaturae(cls.test_db)
        cls.species_per_genus = 3
        cls.num_beings = 10
        cls.trait = 'name'
        
        cls.maker = Maker(
            cls.test_db, cls.species_per_genus, 
            cls.num_beings, cls.trait
        )
        
        (
            cls.fathers, cls.sons, 
            cls.grandsons, cls.great_grandsons
        ) = cls.maker.get() if cls.checkEidolaExist() else cls.maker.make()
            
        
    @classmethod
    def checkEidolaExist(cls):
        exp_creator = 'creators'
        right = cls.species_per_genus - 1
        exp_creature = f'creatures_{right}_{right}_{right}'
        
        with Indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            tables = ix.get_all_tables(cnxn)
            eidola_exist = exp_creator in tables and exp_creature in tables
            
        return eidola_exist
        
    def testUpward(self):
        species = 'creatures_0_0_0'
        creature = self.great_grandsons[0].sample(1)
        next_rung = self.ladder.upward(species, creature)
        
        genus, creator = next_rung[0]
        exp_genus = 'creatures_0_0'
        exp_id = creature[f'{exp_genus}_id'].max()
        self.assertEqual(genus, exp_genus)
        self.assertEqual(creator.id.max(), exp_id)
        
        creator = self.fathers[0].sample(1)
        genus = 'creators'
        exp_empty = self.ladder.upward(genus, creator)
        self.assertEqual(len(exp_empty), 0)
    
    def testDownward(self):
        species = 'creatures_0_0_0'
        creature = self.great_grandsons[0].sample(1)
        next_rung_up = self.ladder.upward(species, creature)
        
        genus, creator_data = next_rung_up[0]
        next_rung_down = self.ladder.downward(genus, creator_data)
        
        species_list = [n[0] for n in next_rung_down]
        self.assertIn(species, species_list)
        creature_data = next_rung_down[species_list.index(species)][1]
        self.assertIn(creature.id.values[0], list(creature_data.id))
        
        exp_empty = self.ladder.downward(species, creature)
        self.assertEqual(len(exp_empty), 0)
    
    def testClimb(self):
        species = 'creatures_0_0_0'
        creature = self.great_grandsons[0].sample(1)
        up = self.ladder.climb(species, creature, 'up')
        genus, creator = up[0]
        self.assertEqual(len(up), 1)
        self.assertEqual('creatures_0_0', genus)
        self.assertIn('id', list(creator.columns))
        self.assertIn('name', list(creator.columns))
        
        down = self.ladder.climb(genus, creator, 'down')
        species, creature = down[0]
        self.assertIn(genus, species)
        
        self.assertEqual(
            ['id', 'name', f'{genus}_id'], list(creature.columns)
        )
        
        self.assertRaises(
            ValueError, self.ladder.climb, 
            genus, creator, 'sideways'
        )
        
        
class TestDendron(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = 'tests/data/test_schemata.db'
        cls.xml_file = 'tests/data/dendron.xml'
        cls.species_per_genus = 3
        cls.num_beings = 10
        cls.trait = 'name'
        
        cls.maker = Maker(
            cls.test_db, cls.species_per_genus, 
            cls.num_beings, cls.trait
        )
        
        (
            cls.fathers, cls.sons, 
            cls.grandsons, cls.great_grandsons
        ) = cls.maker.get() if cls.checkEidolaExist() else cls.maker.make()
        
    @classmethod
    def checkEidolaExist(cls):
        exp_creator = 'creators'
        right = cls.species_per_genus - 1
        exp_creature = f'creatures_{right}_{right}_{right}'
        
        with Indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            tables = ix.get_all_tables(cnxn)
            eidola_exist = exp_creator in tables and exp_creature in tables
            
        return eidola_exist
    
    def testRenderImage(self):
        genus = 'creators'
        creator = self.fathers[0].query('id == 1')
        dendron = Dendron(self.test_db)
        
        rendered = dendron.render_image(genus, creator)
        self.assertIsInstance(rendered, et.ElementTree)
        exp_son = ('creatures_0', self.sons[0].query('id == 1'))
        exp_grandson = ('creatures_0_0', self.grandsons[0].query('id == 1'))
        
        exp_great_grandson = (
            'creatures_0_0_0', self.great_grandsons[0].query('id == 1')
        )
        
        for exp_kind, exp_being in [exp_son, exp_grandson, exp_great_grandson]:
            exp_path = f".//{exp_kind}[@id='{exp_being.id.max()}']"
            creator_element = rendered.find(exp_path)
            self.assertIsInstance(creator_element, et.Element)
    
    def testWriteImage(self):
        genus = 'creators'
        creator = self.fathers[0].query('id == 1')
        dendron = Dendron(self.test_db)
        
        image = dendron.render_image(genus, creator)
        outfile = dendron.write_image(image, self.xml_file, open_browser=False)
        self.assertEqual(self.xml_file, outfile)
    
    def tearDown(self):
        try:
            os.remove(self.xml_file)
        except:
            pass
        
class TestCorpus(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = 'tests/data/test_schemata.db'
        cls.csv_path = 'tests/data/test_corpus.csv'
        cls.species_per_genus = 3
        cls.num_beings = 10
        cls.trait = 'name'
        
        cls.maker = Maker(
            cls.test_db, cls.species_per_genus, 
            cls.num_beings, cls.trait
        )
        
        (
            cls.fathers, cls.sons, 
            cls.grandsons, cls.great_grandsons
        ) = cls.maker.get() if cls.checkEidolaExist() else cls.maker.make()
        
        cls.genus = 'creators'
        cls.creator = cls.fathers[0].query('id == 1')
        cls.corpus = Corpus(cls.test_db, cls.genus, cls.creator, max_depth=4)
        
    @classmethod
    def checkEidolaExist(cls):
        exp_creator = 'creators'
        right = cls.species_per_genus - 1
        exp_creature = f'creatures_{right}_{right}_{right}'
        
        with Indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            tables = ix.get_all_tables(cnxn)
            eidola_exist = exp_creator in tables and exp_creature in tables
            
        return eidola_exist
    
    @classmethod
    def make_frame(cls, columns):
        return pd.DataFrame(columns=columns)
        
    def testGetTrait(self):
        exp_pass = self.make_frame(['id', self.trait])
        trait = self.corpus.get_trait('test_species', exp_pass)
        self.assertEqual(trait, self.trait)
        
        exp_pass = self.make_frame(['id', self.trait, 'creators_id'])
        trait = self.corpus.get_trait('test_species', exp_pass)
        self.assertEqual(trait, self.trait)
        
        exp_fail = self.make_frame(['id', 'creators_id'])
        
        self.assertRaises(
            ValueError, self.corpus.get_trait, 
            'test_species', exp_fail
        )
        
        exp_fail = self.make_frame([
            'id', self.trait, 'extra_trait', 'creators_id'
        ])
        
        self.assertRaises(
            ValueError, self.corpus.get_trait, 
            'test_species', exp_fail
        )
     
    def testMakeMember(self):
        members = self.corpus.make_member(
            None, pd.DataFrame(), self.genus, self.creator
        )
        
        exp_columns = [
            'genus', 'creator_id', 
            'species', 'creature_id', 
            'trait', 'expression'
        ]
        
        self.assertEqual(list(members.columns), exp_columns)
        self.assertEqual(members.shape[0], self.creator.shape[0])
        self.assertIsNone(members.genus.values[0])
        self.assertIsNone(members.creator_id.values[0])
        self.assertEqual(members.species.values[0], self.genus)
        
        self.assertEqual(
            members.creature_id.values[0], self.creator.id.values[0]
        )
        
        self.assertEqual(members.trait.values[0], self.trait)
        
        self.assertEqual(
            members.expression.values[0], self.creator[self.trait].values[0]
        )
        
    def testMakeLimb(self):
        self.corpus.max_depth = 1
        limb = self.corpus.make_limbs(self.genus, self.creator, 0)
        limb = pd.concat(limb, axis=0)
        exp_genus = {self.genus}
        exp_creator_id = {1}
        exp_species = {f'creatures_{i}' for i in range(self.species_per_genus)}
        exp_trait = {self.trait}
        self.assertEqual(set(limb.genus), exp_genus)
        self.assertEqual(set(limb.creator_id), exp_creator_id)
        self.assertEqual(set(limb.species), exp_species)
        self.assertEqual(set(limb.trait), exp_trait)
        
        self.corpus.max_depth = 0
        limb = self.corpus.make_limbs(self.genus, self.creator, 0)
        self.assertFalse(limb)
        
        genus = 'creatures_0_0_0'
        creator = self.great_grandsons[0].iloc[[0]]
        limb = self.corpus.make_limbs(genus, creator, 0)
        self.assertFalse(limb)
        
        self.corpus.max_depth = 2
        limb = self.corpus.make_limbs(self.genus, self.creator, 0)
        limb = pd.concat(limb, axis=0)
        exp_species = set()
        
        for i in range(self.species_per_genus):
            exp_species = exp_species.union({f'creatures_{i}'})
            
            for j in range(self.species_per_genus):
                exp_species = exp_species.union({f'creatures_{i}_{j}'})
        
        self.assertEqual(set(limb.species), exp_species)
        
    def testAssemble(self):
        self.corpus.max_depth = 5
        corpus = self.corpus.assemble()
        exp_index = [i for i in range(corpus.shape[0])]
        self.assertEqual(list(corpus.index), exp_index)
        exp_species = {'creators'}
        
        for i in range(self.species_per_genus):
            exp_species = exp_species.union({f'creatures_{i}'})
            
            for j in range(self.species_per_genus):
                exp_species = exp_species.union({f'creatures_{i}_{j}'})
                
                for k in range(self.species_per_genus):
                    exp_species = exp_species.union({f'creatures_{i}_{j}_{k}'})
                    
        self.assertEqual(set(corpus.species), exp_species)
        
    def testToCSV(self):
        self.corpus.max_depth = 5
        corpus = self.corpus.assemble()
        file_path = self.corpus.to_csv(corpus, self.csv_path, index=False)
        self.assertEqual(file_path, self.csv_path)
    
    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls.csv_path)
        except:
            pass
        
class TestDiktua(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_db = 'tests/data/test_schemata.db'
        cls.csv_path = 'tests/data/test_diktua.csv'
        cls.html_path = 'tests/data/test_diktua.html'
        cls.species_per_genus = 3
        cls.num_beings = 10
        cls.trait = 'name'
        
        cls.maker = Maker(
            cls.test_db, cls.species_per_genus, 
            cls.num_beings, cls.trait
        )
        
        (
            cls.fathers, cls.sons, 
            cls.grandsons, cls.great_grandsons
        ) = cls.maker.get() if cls.checkEidolaExist() else cls.maker.make()
        
        cls.genus = 'creators'
        cls.creator = cls.fathers[0].sample(1)
        cls.corpus = Corpus(cls.test_db, cls.genus, cls.creator, 1).assemble()
        cls.self_edges = False
        
        cls.diktua = Diktua(
            cls.corpus,
            as_nodes='species', 
            as_edges='genus',
            self_edges=cls.self_edges
        )
    
    @classmethod
    def checkEidolaExist(cls):
        exp_creator = 'creators'
        right = cls.species_per_genus - 1
        exp_creature = f'creatures_{right}_{right}_{right}'
        
        with Indexia(cls.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            tables = ix.get_all_tables(cnxn)
            eidola_exist = exp_creator in tables and exp_creature in tables
            
        return eidola_exist
    
    @classmethod
    def get_expected_edges(cls, self_edges):
        sons = [f'creatures_{i}' for i in range(cls.species_per_genus)]
        
        if self_edges:
            exp_edges = {
                tuple(sorted(i)) for i in itertools.permutations(sons, 2)
            }
        else:
            exp_edges = {
                tuple(sorted(i)) for i in itertools.combinations(sons, 2)
            }
            
        return exp_edges
        
    def testGetGraphElements(self):
        exp_nodes = set(self.corpus.species.unique()) - {self.genus}
        exp_edges = self.get_expected_edges(self.self_edges)
        nodes, edges = self.diktua.get_graph_elements()
        edges = [tuple(sorted(e)) for e in edges]
        self.assertEqual(set(nodes), exp_nodes)
        self.assertEqual(set(edges), exp_edges)
        
        exp_edges = self.get_expected_edges(not self.self_edges)
        self.diktua.self_edges = not self.self_edges
        nodes, edges = self.diktua.get_graph_elements()
        edges = [tuple(sorted(e)) for e in edges]
        self.assertEqual(set(edges), exp_edges)
    
    def testMakeUndirectedGraph(self):
        exp_nodes = set(self.corpus.species.unique()) - {self.genus}
        exp_edges = self.get_expected_edges(self.self_edges)
        self.diktua.self_edges = self.self_edges
        G = self.diktua.make_undirected_graph()
        self.assertEqual(set(G.nodes), exp_nodes)        
        self.assertEqual(set([tuple(sorted(e)) for e in G.edges]), exp_edges)
    
    def testGetNodeInfo(self):
        node_connections, node_titles = self.diktua.get_node_info()
        exp_connections = {2}
        exp_titles = {'(2)'}
        self.assertEqual(set(node_connections.values()), exp_connections)
        self.assertEqual(set(node_titles.values()), exp_titles)
    
    def testGetNodeSizes(self):
        min_size = 7
        max_size = 49
        max_connections = 2
        
        connections, _ = self.diktua.get_node_info()
        
        node_sizes = self.diktua.get_node_sizes(
            connections, min_size, max_size
        )
        
        exp_sizes = {max_size}
        self.assertEqual(set(node_sizes.values()), exp_sizes)
        
        connections['creatures_0'] = 0
        
        node_sizes = self.diktua.get_node_sizes(
            connections, min_size, max_size
        )
        
        exp_sizes = {min_size, max_size}
        self.assertEqual(set(node_sizes.values()), exp_sizes)
        
        connections['creatures_1'] = 1
        
        node_sizes = self.diktua.get_node_sizes(
            connections, min_size, max_size
        )
        
        mid_size = min_size + round(
            (max_size - min_size) * (1 / max_connections)
        )
        
        exp_sizes = {min_size, mid_size, max_size}
        self.assertEqual(set(node_sizes.values()), exp_sizes)
    
    def testStyleNodes(self):
        min_size = 7
        max_size = 49
        max_connections = 2
        exp_title = f'({max_connections})'
        
        exp_result = {s: {
            'size': max_size, 
            'title': exp_title
        } for s in ['creatures_0', 'creatures_1', 'creatures_2']}
        
        self.diktua.style_nodes(min_size=min_size, max_size=max_size)
        result = self.diktua.G.nodes.data()
        self.assertDictEqual(dict(result), exp_result)
    
    def testPlot(self):
        plot, path = self.diktua.plot()
        self.assertIsInstance(plot, Network)
        self.assertIsNone(path)
        
        plot, path = self.diktua.plot(self.html_path)
        self.assertTrue(os.path.isfile(path))
    
    def testToCSV(self):
        csv_path = self.diktua.to_csv(self.csv_path)
        self.assertTrue(os.path.isfile(csv_path))
    
    @classmethod
    def tearDownClass(cls):
        try:
            os.remove(cls.csv_path)
            os.remove(cls.html_path)
        except:
            pass


if __name__ == '__main__':
    ut.main()