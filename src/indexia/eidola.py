'''
Make sample creators & creatures.

'''
from indexia.indexia import Indexia
from datetime import datetime as dt, timedelta as td
from typing import Any
import random
import sqlite3
import pandas


class Maker:
    '''
    Fashion any number of creators & creatures for testing.
    
    '''
    def __init__(
        self,
        test_db: str,
        species_per_genus: int,
        num_beings: int,
        trait: str
    ) -> None:
        '''
        Create a Maker instance.

        Parameters
        ----------
        test_db : str
            Path to a test database file.
        species_per_genus : int
            Number of creature tables to be added to the 
            genus.
        num_beings : int
            Number of creature records to be created.
        trait : str
            Text attribute of the creatures.

        Returns
        -------
        None.

        '''
        self.test_db: str = test_db
        self.species_per_genus: int = species_per_genus
        self.num_beings: int = num_beings
        self.trait: str = trait
        
    def make_creators(
        self,
        ix: Indexia,
        cnxn: sqlite3.Connection,
        genus: str
    ) -> pandas.DataFrame:
        '''
        Make creator beings.
        
        The number of beings specified by num_beings
        will be created in a table named genus.

        Parameters
        ----------
        ix : indexia.indexia.Indexia
            An Indexia instance.
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table.

        Returns
        -------
        creators : pandas.DataFrame
            Dataframe of creator data.

        '''
        for i in range(self.num_beings):
            ix.add_creator(
                cnxn, genus, 
                self.trait, f'{genus}_{i}'
            )
            
        sql = 'SELECT * FROM creators;'
        creators: pandas.DataFrame = ix.get_df(cnxn, sql)
        
        return creators
        
    def make_creatures(
        self,
        ix: Indexia,
        cnxn: sqlite3.Connection,
        genus: str,
        species: str
    ) -> pandas.DataFrame:
        '''
        Make sample creatures with a given genus & species.
        
        A creature table named species is created, & num_beings 
        creature records are added to the table. Each creature 
        has the attribute trait.
    
        Parameters
        ----------
        ix : indexia.indexia.Indexia
            An Indexia instance.
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table.
        species : str
            Name of the creature (child) table.
    
        Returns
        -------
        creatures : pandas.DataFrame
            Dataframe of creature data.
    
        '''
        for i in range(self.num_beings):
            creator: pandas.DataFrame = ix.get_by_id(cnxn, genus, i + 1)
            
            ix.add_creature(
                cnxn, genus, creator, 
                species, self.trait, f'{species}_{i}'
            )
        
        sql: str = f'SELECT * FROM {species};'
        creatures: pandas.DataFrame = ix.get_df(cnxn, sql)
        
        return creatures
    
    def make_species(
        self,
        ix: Indexia,
        cnxn: sqlite3.Connection,
        genus: str,
        species_prefix: str
    ) -> list[pandas.DataFrame]:
        '''
        Make one or more species of a given genus.
        
        The number of species created for in the genus is 
        given by species_per_genus. For each species created,
        num_beings creature records are added to the species, 
        each having the attribute trait.
    
        Parameters
        ----------
        ix : indexia.indexia.Indexia
            An Indexia instance.
        cnxn : sqlite3.Connection
            A database connection.
        genus : str
            Name of the creator (parent) table.
        species_prefix : str
            Prefix of the creature (child) table names.
        species : str
            Name of the creature (child) table.
    
        Returns
        -------
        species : list[pandas.DataFrame]
            List of dataframes containing creature data.
    
        '''
        species: list[pandas.DataFrame] = []
        
        for i in range(self.species_per_genus):
            species_name: str = f'{species_prefix}_{i}'
            species += [self.make_creatures(ix, cnxn, genus, species_name)]
            
        return species
        
    def make(
        self
    ) -> tuple[list[pandas.DataFrame], list[pandas.DataFrame], list[pandas.DataFrame], list[pandas.DataFrame]]:
        '''
        Make test data.
        
        Makes a single creator table & 3 generations of 
        creature tables. Each generation has species_per_genus 
        tables, with num_beings creatures in each table. The 
        creators & creatures all have the attribute trait.
    
        Returns
        -------
        fathers : list[pandas.DataFrame]
            List containing a single dataframe of creator 
            data.
        sons : list[pandas.DataFrame]
            List containing species_per_genus dataframes 
            of creature data.
        grandsons : list[pandas.DataFrame]
            List containing (species_per_genus)^2 dataframes 
            of creature data.
        great_grandsons : list[pandas.DataFrame]
            List containing (species_per_genus)^3 dataframes 
            of creature data.

        '''
        with Indexia(self.test_db) as ix:
            cnxn: sqlite3.Connection = ix.open_cnxn(ix.db)
            genus = 'creators'

            fathers: list[pandas.DataFrame] = [
                self.make_creators(ix, cnxn, genus)
            ]

            species_prefix: str = 'creatures'

            sons: list[pandas.DataFrame] = self.make_species(
                ix, cnxn, genus, species_prefix
            )

            grandsons: list[pandas.DataFrame] = []
            
            for i in range(self.species_per_genus):
                genus: str = f'creatures_{i}'
                species_prefix = f'creatures_{i}'
                
                grandsons += self. make_species(
                    ix, cnxn, genus, species_prefix
                )
                
            great_grandsons: list[pandas.DataFrame] = []
            
            for i in range(self.species_per_genus):
                for j in range(self.species_per_genus):
                    genus = f'creatures_{i}_{j}'
                    species_prefix = f'creatures_{i}_{j}'
                    
                    great_grandsons += self.make_species(
                        ix, cnxn, genus, species_prefix
                    )
                                
        return fathers, sons, grandsons, great_grandsons
    
    def get(
        self
    ) -> tuple[list[pandas.DataFrame], list[pandas.DataFrame], list[pandas.DataFrame], list[pandas.DataFrame]]:
        '''
        Get test data.

        Returns
        -------
        fathers : list[pandas.DataFrame]
            List containing a single dataframe of creator 
            data.
        sons : list[pandas.DataFrame]
            List containing species_per_genus dataframes 
            of creature data.
        grandsons : list[pandas.DataFrame]
            List containing (species_per_genus)^2 dataframes 
            of creature data.
        great_grandsons : list[pandas.DataFrame]
            List containing (species_per_genus)^3 dataframes 
            of creature data.

        '''
        with Indexia(self.test_db) as ix:
            cnxn: sqlite3.Connection = ix.open_cnxn(ix.db)
            sql = 'SELECT * FROM creators;'
            fathers: list[pandas.DataFrame] = [ix.get_df(cnxn, sql)]
            sons: list[pandas.DataFrame] = []
            grandsons: list[pandas.DataFrame]  = []
            great_grandsons: list[pandas.DataFrame]  = []
            
            for i in range(self.species_per_genus):
                sql: str = f'SELECT * FROM creatures_{i};'
                sons += [ix.get_df(cnxn, sql)]
                
                for j in range(self.species_per_genus):
                    sql = f'SELECT * FROM creatures_{i}_{j};'
                    grandsons += [ix.get_df(cnxn, sql)]
                    
                    for k in range(self.species_per_genus):
                        sql = f'SELECT * FROM creatures_{i}_{j}_{k};'
                        great_grandsons += [ix.get_df(cnxn, sql)]
                        
            return fathers, sons, grandsons, great_grandsons


class Templates:
    '''
    Create template indexia objects.
    
    '''
    def __init__(
        self,
        db: str
    ) -> None:
        '''
        Creates a Templates instance.

        Parameters
        ----------
        db : str
            Path to the indexia database file.

        Returns
        -------
        None.

        '''
        self.db: str = db
        
    def show_templates(
        self
    ) -> dict[str, dict[str, Any]]:
        '''
        Show available templates

        Returns
        -------
        templates : dict
            Dictionary of available template names & table structures.

        '''
        templates: dict[str, dict[str, Any]] = {
            'philosophy': {'philosophers': {'works': {'topics'}}},
            'zettelkasten': {'scribes': {'libraries': {'cards': {'keywords'}}}}
        }
        
        print('Available templates:')
        [print(f'\n{t}:', templates[t], sep='\n\t') for t in templates]
        
        return templates
        
    def build_template(
        self,
        template_name: str
    ) -> dict[str, pandas.DataFrame]:
        '''
        Create objects for the given template.

        Parameters
        ----------
        template_name : str
            Name of the template to build.

        Raises
        ------
        ValueError
            If template_name is not a valid template, raise 
            a ValueError.

        Returns
        -------
        objects : dict[str, pandas.DataFrame]
            Dictionary mapping table names to their DataFrame objects.

        '''
        objects: dict[str, pandas.DataFrame] = {}
        
        if template_name == 'philosophy':
            with Indexia(self.db) as ix:
                cnxn: sqlite3.Connection = ix.open_cnxn(ix.db)
                
                plato: pandas.DataFrame = ix.add_creator(
                    cnxn, 'philosophers', 'name', 'Plato'
                )
                
                aristotle: pandas.DataFrame = ix.add_creator(
                    cnxn, 'philosophers', 'name', 'Aristotle'
                )
                
                apology: pandas.DataFrame = ix.add_creature(
                    cnxn, 'philosophers', plato, 'works', 
                    'title', 'Apology of Socrates'
                )
                
                symposium: pandas.DataFrame = ix.add_creature(
                    cnxn, 'philosophers', plato, 
                    'works', 'title', 'Symposium'
                )
                
                republic: pandas.DataFrame = ix.add_creature(
                    cnxn, 'philosophers', plato, 
                    'works', 'title', 'Republic'
                )
                
                on_the_heavens: pandas.DataFrame = ix.add_creature(
                    cnxn, 'philosophers', aristotle, 
                    'works', 'title', 'On the Heavens'
                )
                
                topics: pandas.DataFrame = ix.add_creature(
                    cnxn, 'philosophers', aristotle, 
                    'works', 'title', 'Topics'
                )
                
                on_the_soul: pandas.DataFrame = ix.add_creature(
                    cnxn, 'philosophers', aristotle, 
                    'works', 'title', 'On the Soul'
                )
                
                ix.add_creature(
                    cnxn, 'works', apology, 
                    'topics', 'name', 'civics'
                )
                
                ix.add_creature(
                    cnxn, 'works', symposium, 
                    'topics', 'name', 'love'
                )
                
                ix.add_creature(
                    cnxn, 'works', republic, 
                    'topics', 'name', 'civics'
                )
                
                ix.add_creature(
                    cnxn, 'works', on_the_heavens, 
                    'topics', 'name', 'cosmology'
                )
                
                ix.add_creature(
                    cnxn, 'works', topics, 
                    'topics', 'name', 'logic'
                )
                
                ix.add_creature(
                    cnxn, 'works', on_the_soul, 
                    'topics', 'name', 'psychology'
                )
                
                ix.add_creature(
                    cnxn, 'works', apology, 
                    'topics', 'name', 'civics'
                )
                
                philosophers: pandas.DataFrame = ix.get_df(
                    cnxn, 'SELECT * FROM philosophers;'
                )

                objects['philosophers'] = philosophers
                
                works: pandas.DataFrame = ix.get_df(
                    cnxn, 'SELECT * FROM works;'
                )

                objects['works'] = works
                
                topics = ix.get_df(cnxn, 'SELECT * FROM topics;')
                objects['topics'] = topics
                
        elif template_name == 'zettelkasten':
            with Indexia(self.db) as ix:
                cnxn = ix.open_cnxn(ix.db)
                
                scribe: pandas.DataFrame = ix.add_creator(
                    cnxn, 'scribes', 'name', 'Grammateus'
                )
                
                first: pandas.DataFrame = ix.add_creature(
                    cnxn, 'scribes', scribe, 'libraries', 'name', 'First'
                )
                
                second: pandas.DataFrame = ix.add_creature(
                    cnxn, 'scribes', scribe, 'libraries', 'name', 'Second'
                )
                
                now: dt = dt.now()
                
                keywords: list[str] = [
                    'writing', 'inscription', 'zettelkasten',
                    'mnemotechnic', 'topic', 'category',
                    'antinomy', 'reason', 'anatomy',
                    'science', 'logic', 'apodeictic'
                ]
                
                for library in [first, second]:
                    for _ in range(1, 4):
                        created: dt = now + td(minutes=1)
                        created_str: str = created.strftime('%Y-%m-%d-%H-%M')
                        
                        card: pandas.DataFrame = ix.add_creature(
                            cnxn, 'libraries', library, 
                            'cards', 'created', created_str
                        )
                        
                        for keyword in random.sample(keywords, 3):
                            ix.add_creature(
                                cnxn, 'cards', card,
                                'keywords', 'keyword', keyword
                            )
                
                scribes: pandas.DataFrame = ix.get_df(
                    cnxn, 'SELECT * FROM scribes;'
                )

                objects['scribes'] = scribes
                
                libraries: pandas.DataFrame = ix.get_df(
                    cnxn, 'SELECT * FROM libraries;'
                )

                objects['libraries'] = libraries
                
                cards: pandas.DataFrame = ix.get_df(
                    cnxn, 'SELECT * FROM cards;'
                )

                objects['cards'] = cards
                
                keywords_df: pandas.DataFrame = ix.get_df(
                    cnxn, 'SELECT * FROM keywords;'
                )

                objects['keywords'] = keywords_df
                
        else:
            self.show_templates()
            raise ValueError(f'Found no template {template_name}.')
            
        return objects