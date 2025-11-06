from indexia.eidola import Maker
from indexia.indexia import Indexia
from indexia.schemata import Corpus, Dendron, Diktua, ScalaNaturae
from networkx.classes.reportviews import NodeDataView
from pyvis.network import Network # type: ignore
from sqlite3 import Connection
from typing import Any
import itertools
import os
import pandas as pd
import unittest as ut
import xml.etree.ElementTree as et


class TestSchemata(ut.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.test_db: str = 'tests/data/test_schemata.db'
        cls.xml_file: str = 'tests/data/dendron.xml'
        cls.csv_path: str = 'tests/data/test_corpus.csv'
        cls.ladder: ScalaNaturae = ScalaNaturae(cls.test_db)
        cls.species_per_genus: int = 3
        cls.num_beings: int = 5
        cls.trait: str = 'name'
        cls.fathers: list[pd.DataFrame] = []
        cls.sons: list[pd.DataFrame] = []
        cls.grandsons: list[pd.DataFrame] = []
        cls.great_grandsons: list[pd.DataFrame] = []
        
        cls.maker = Maker(
            cls.test_db,
            cls.species_per_genus, 
            cls.num_beings,
            cls.trait
        )
        
        (
            cls.fathers,
            cls.sons, 
            cls.grandsons,
            cls.great_grandsons
        ) = cls.maker.get() if cls.checkEidolaExist() else cls.maker.make()

        cls.genus: str = 'creators'
        cls.creators: pd.DataFrame = cls.fathers[0]
        cls.corpus = Corpus(cls.test_db, cls.genus, cls.creators, max_depth=1)
        
    @classmethod
    def checkEidolaExist(cls) -> bool:
        exp_creator: str = 'creators'
        right: int = cls.species_per_genus - 1
        exp_creature: str = f'creatures_{right}_{right}_{right}'
        
        with Indexia(cls.test_db) as ix:
            cnxn: Connection = ix.open_cnxn(ix.db)
            tables: list[str] = ix.get_all_tables(cnxn)

            eidola_exist: bool = (
                exp_creator in tables and exp_creature in tables
            )
            
        return eidola_exist

class TestScalaNaturae(TestSchemata):
    def testUpward(self) -> None:
        species = 'creatures_0_0_0'
        creature: pd.DataFrame = self.great_grandsons[0].sample(1) # type: ignore
        
        next_rung: list[tuple[str, pd.DataFrame]] = self.ladder.upward(
            species,
            creature
        )
        
        genus: str = next_rung[0][0]
        creator: pd.DataFrame = next_rung[0][1]
        exp_genus: str = 'creatures_0_0'
        exp_id: int = creature[f'{exp_genus}_id'].max()
        self.assertEqual(genus, exp_genus)
        self.assertEqual(creator.id.max(), exp_id)
        
        creator: pd.DataFrame = self.fathers[0].sample(1) # type: ignore
        genus: str = 'creators'
        
        exp_empty: list[tuple[str, pd.DataFrame]] = self.ladder.upward(
            genus,
            creator
        )

        self.assertEqual(len(exp_empty), 0)
    
    def testDownward(self) -> None:
        species: str = 'creatures_0_0_0'
        creature: pd.DataFrame = self.great_grandsons[0].sample(1) # type: ignore
        
        next_rung_up: list[tuple[str, pd.DataFrame]] = self.ladder.upward(
            species,
            creature
        )
        
        genus: str = next_rung_up[0][0]
        creator_data: pd.DataFrame = next_rung_up[0][1]
        
        next_rung_down: list[tuple[str, pd.DataFrame]] = self.ladder.downward(
            genus,
            creator_data
        )
        
        species_list: list[str] = [n[0] for n in next_rung_down]
        self.assertIn(species, species_list)

        creature_data: pd.DataFrame = next_rung_down[
            species_list.index(species)
        ][1]
        
        self.assertIn(list(creature.id)[0], list(creature_data.id))
        
        exp_empty: list[tuple[str, pd.DataFrame]] = self.ladder.downward(
            species,
            creature
        )

        self.assertEqual(len(exp_empty), 0)
    
    def testClimb(self) -> None:
        species = 'creatures_0_0_0'
        creature: pd.DataFrame = self.great_grandsons[0].sample(1) # type: ignore
        
        up: list[tuple[str, pd.DataFrame]] = self.ladder.climb(
            species,
            creature,
            'up'
        )

        genus: str = up[0][0]
        creator: pd.DataFrame = up[0][1]
        self.assertEqual(len(up), 1)
        self.assertEqual('creatures_0_0', genus)
        self.assertIn('id', list(creator.columns))
        self.assertIn('name', list(creator.columns))
        
        down: list[tuple[str, pd.DataFrame]] = self.ladder.climb(
            genus,
            creator,
            'down'
        )

        species: str = down[0][0]
        creature: pd.DataFrame = down[0][1]
        self.assertIn(genus, species)
        
        self.assertEqual(
            ['id', 'name', f'{genus}_id'], list(creature.columns)
        )
        
        self.assertRaises(
            ValueError, self.ladder.climb, 
            genus, creator, 'sideways'
        )
        
        
class TestDendron(TestSchemata):
    def testRenderImage(self) -> None:
        genus: str = 'creators'
        creator: pd.DataFrame = self.fathers[0].loc[self.fathers[0]['id'] == 1]
        dendron = Dendron(self.test_db)
        
        rendered: et.ElementTree[et.Element[str] | None] = dendron.render_image(
            genus,
            creator
        )

        self.assertIsInstance(rendered, et.ElementTree)

        exp_son: tuple[str, pd.DataFrame] = (
            'creatures_0',
            self.sons[0].loc[self.sons[0]['id'] == 1]
        )

        exp_grandson: tuple[str, pd.DataFrame] = (
            'creatures_0_0',
            self.grandsons[0].loc[self.grandsons[0]['id'] == 1]
        )
        
        exp_great_grandson: tuple[str, pd.DataFrame] = (
            'creatures_0_0_0',
            self.great_grandsons[0].loc[self.great_grandsons[0]['id'] == 1]
        )
        
        exp_creatures: list[tuple[str, pd.DataFrame]] = [
            exp_son,
            exp_grandson,
            exp_great_grandson
        ]
        
        for exp_species, exp_creature in exp_creatures:
            exp_path: str = f".//{exp_species}[@id='{exp_creature.id.max()}']"
            creator_element: et.Element[str] | None = rendered.find(exp_path)
            self.assertIsInstance(creator_element, et.Element)
    
    def testWriteImage(self) -> None:
        genus: str = 'creators'
        creator: pd.DataFrame = self.fathers[0].loc[self.fathers[0]['id'] == 1]
        dendron: Dendron = Dendron(self.test_db)
        
        image: et.ElementTree[et.Element[str] | None] = dendron.render_image(
            genus,
            creator
        )

        outfile: str = dendron.write_image(
            image,
            self.xml_file,
            open_browser=False
        )

        self.assertEqual(self.xml_file, outfile)
    
    def tearDown(self) -> None:
        try:
            os.remove(self.xml_file)
        except:
            pass
        
class TestCorpus(TestSchemata):
    @classmethod
    def make_frame(cls, columns: list[str]) -> pd.DataFrame:
        return pd.DataFrame(columns=columns)
        
    def testGetTrait(self) -> None:
        trait: str = self.corpus.get_trait('creatures_0')
        self.assertEqual(trait, self.trait)
        
        trait = self.corpus.get_trait('creatures_0_0')
        self.assertEqual(trait, self.trait)
        
        self.assertRaises(ValueError, self.corpus.get_trait, 'exp_fail')
        
    def testMakeMember(self) -> None:
        creator: pd.DataFrame = self.creators.loc[self.creators['id'] == 1]
        
        members: pd.DataFrame = self.corpus.make_member(
            None, pd.DataFrame(), self.genus, creator
        )
        
        exp_columns: list[str] = [
            'genus', 'creator_id', 
            'species', 'creature_id', 
            'trait', 'expression'
        ]
        
        self.assertEqual(list(members.columns), exp_columns)
        self.assertEqual(members.shape[0], creator.shape[0])
        self.assertIsNone(list(members.genus)[0])
        self.assertIsNone(list(members.creator_id)[0])
        self.assertEqual(list(members.species)[0], self.genus)
        
        self.assertEqual(
            list(members.creature_id)[0], list(creator.id)[0]
        )
        
        self.assertEqual(list(members.trait)[0], self.trait)
        
        self.assertEqual(
            list(members.expression)[0], list(creator[self.trait])[0]
        )
        
    def testMakeLimb(self) -> None:
        self.corpus.max_depth = 1
        creator: pd.DataFrame = self.creators.loc[self.creators['id'] == 1]

        limbs: list[pd.DataFrame] = self.corpus.make_limbs(
            self.genus, creator, 0
        )
        
        limb: pd.DataFrame = pd.concat(limbs, axis=0)
        exp_genus: set[str] = {self.genus}
        exp_creator_id: set[int] = {1}

        exp_species: set[str] = {
            f'creatures_{i}' for i in range(self.species_per_genus)
        }

        exp_trait: set[str] = {self.trait}
        self.assertEqual(set(limb.genus), exp_genus)
        self.assertEqual(set(limb.creator_id), exp_creator_id)
        self.assertEqual(set(limb.species), exp_species)
        self.assertEqual(set(limb.trait), exp_trait)
        
        self.corpus.max_depth = 0
        limbs = self.corpus.make_limbs(self.genus, self.creators, 0)
        self.assertFalse(limbs)
        
        genus = 'creatures_0_0_0'
        creator = self.great_grandsons[0].iloc[[0]]
        limbs = self.corpus.make_limbs(genus, creator, 0)
        self.assertFalse(limbs)
        
        self.corpus.max_depth = 2
        limbs = self.corpus.make_limbs(self.genus, self.creators, 0)
        limb = pd.concat(limbs, axis=0)
        exp_species = set()
        
        for i in range(self.species_per_genus):
            exp_species = exp_species.union({f'creatures_{i}'})
            
            for j in range(self.species_per_genus):
                exp_species = exp_species.union({f'creatures_{i}_{j}'})
        
        self.assertEqual(set(limb.species), exp_species)
        
    def testAssemble(self) -> None:
        self.corpus.max_depth = 5
        corpus: pd.DataFrame = self.corpus.assemble()
        exp_index: list[int] = [i for i in range(corpus.shape[0])]
        self.assertEqual(list(corpus.index), exp_index)
        exp_species: set[str] = {'creators'}
        
        for i in range(self.species_per_genus):
            exp_species = exp_species.union({f'creatures_{i}'})
            
            for j in range(self.species_per_genus):
                exp_species = exp_species.union({f'creatures_{i}_{j}'})
                
                for k in range(self.species_per_genus):
                    exp_species = exp_species.union({f'creatures_{i}_{j}_{k}'})
                    
        self.assertEqual(set(corpus.species), exp_species)
        
    def testToCSV(self) -> None:
        self.corpus.max_depth = 5
        corpus: pd.DataFrame = self.corpus.assemble()

        file_path: str = self.corpus.to_csv(
            corpus,
            self.csv_path,
            index={}
        )

        self.assertEqual(file_path, self.csv_path)
    
    @classmethod
    def tearDownClass(cls) -> None:
        try:
            os.remove(cls.csv_path)
        except:
            pass
        
class TestDiktua(TestSchemata):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.csv_path = 'tests/data/test_diktua.csv'
        cls.html_path = 'tests/data/test_diktua.html'
        cls.corpus_df: pd.DataFrame = cls.corpus.assemble()
        cls.self_edges = False
        
        cls.diktua = Diktua(
            cls.corpus_df,
            as_nodes='species', 
            as_edges='genus',
            self_edges=cls.self_edges
        )
    
    @classmethod
    def get_expected_edges(
            cls, self_edges: bool
        ) -> set[tuple[str, ...] | tuple[str, str]]:
        sons: list[str] = [
            f'creatures_{i}' for i in range(cls.species_per_genus)
        ]
        
        exp_edges: set[tuple[str, ...]] = {
            tuple(sorted(i)) for i in itertools.combinations(sons, 2)
        }
        
        if self_edges:
            exp_edges = exp_edges.union({(s, s) for s in sons})
            
        return exp_edges
        
    def testGetGraphElements(self) -> None:
        exp_nodes: set[str] = set(list(self.corpus_df.species)) - {self.genus}
        exp_edges: set[tuple[str, ...] | tuple[str, str]] = self.get_expected_edges(self.self_edges)
        self.diktua.self_edges = self.self_edges
        elements: tuple[list[Any], list[tuple[Any, Any]]] = self.diktua.get_graph_elements()
        nodes: list[Any] = elements[0]
        edges: list[tuple[Any, Any]] = elements[1]
        edges = [tuple(sorted(e)) for e in edges]
        self.assertEqual(set(nodes), exp_nodes)
        self.assertEqual(set(edges), exp_edges)
        
        exp_edges = self.get_expected_edges(not self.self_edges)
        self.diktua.self_edges = not self.self_edges
        nodes, edges = self.diktua.get_graph_elements()
        edges = [tuple(sorted(e)) for e in edges]
        self.assertEqual(set(edges), exp_edges)
    
    def testMakeUndirectedGraph(self) -> None:
        exp_nodes: set[str] = set(list(self.corpus_df.species)) - {self.genus}
        exp_edges: set[tuple[str, ...] | tuple[str, str]] = self.get_expected_edges(self.self_edges)
        self.diktua.self_edges = self.self_edges
        self.diktua.make_undirected_graph()
        self.assertEqual(set(self.diktua.G.nodes), exp_nodes) # type: ignore

        self.assertEqual(set([
            tuple(sorted(e)) for e in self.diktua.G.edges  # type: ignore
        ]), exp_edges)
    
    def testGetNodeInfo(self) -> None:
        node_info: tuple[dict[Any, int], dict[Any, str]] = self.diktua.get_node_info()
        node_connections: dict[Any, int] = node_info[0]
        node_titles: dict[Any, str] = node_info[1]
        exp_connections: set[int] = {2}
        exp_titles: set[str] = {'(2)'}
        self.assertEqual(set(node_connections.values()), exp_connections)
        self.assertEqual(set(node_titles.values()), exp_titles)
    
    def testGetNodeSizes(self) -> None:
        min_size: int = 7
        max_size: int = 49
        max_connections: int = 2
        
        connections: dict[Any, int]= self.diktua.get_node_info()[0]
        
        node_sizes: dict[Any, int] = self.diktua.get_node_sizes(
            connections, min_size, max_size
        )
        
        exp_sizes: set[int] = {max_size}
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
        
        mid_size: int = min_size + round(
            (max_size - min_size) * (1 / max_connections)
        )
        
        exp_sizes = {min_size, mid_size, max_size}
        self.assertEqual(set(node_sizes.values()), exp_sizes)
    
    def testStyleNodes(self) -> None:
        min_size: int = 7
        max_size: int = 49
        max_connections: int = 2
        exp_title: str = f'({max_connections})'
        
        exp_result: dict[str, dict[str, int | str]] = {s: {
            'size': max_size, 
            'title': exp_title
        } for s in ['creatures_0', 'creatures_1', 'creatures_2']}
        
        self.diktua.style_nodes(min_size=min_size, max_size=max_size)
        result: NodeDataView[Any] = self.diktua.G.nodes.data() # type: ignore
        self.assertDictEqual(dict(result), exp_result)
    
    def testPlot(self) -> None:
        plot_result: tuple[Network, str | None] = self.diktua.plot()
        plot: Network = plot_result[0]
        path: str | None = plot_result[1]
        self.assertIsInstance(plot, Network)
        self.assertIsNone(path)
        
        plot, path = self.diktua.plot(self.html_path)
        self.assertTrue(os.path.isfile(str(path)))
    
    def testToCSV(self) -> None:
        csv_path: str = self.diktua.to_csv(self.csv_path)
        self.assertTrue(os.path.isfile(csv_path))
    
    @classmethod
    def tearDownClass(cls) -> None:
        try:
            os.remove(cls.csv_path)
            os.remove(cls.html_path)
        except:
            pass


if __name__ == '__main__':
    ut.main()