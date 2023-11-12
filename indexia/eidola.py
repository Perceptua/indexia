'''
eidola

Make sample creators & creatures.

'''
from indexia.indexia import Indexia


class Maker:
    '''
    Fashion any number of creators & creatures for testing.
    
    '''
    def __init__(self, test_db, species_per_genus, num_beings, trait):
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
        self.test_db = test_db
        self.species_per_genus = species_per_genus
        self.num_beings = num_beings
        self.trait = trait
        
    def make_creators(self, ix, cnxn, genus):
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
        creators : list(pandas.DataFrame)
            Dataframe of creator data.

        '''
        for i in range(self.num_beings):
            ix.add_creator(
                cnxn, genus, 
                self.trait, f'{genus}_{i}'
            )
            
        sql = 'SELECT * FROM creators;'
        creators = ix.get_df(cnxn, sql)
        
        return creators
        
    def make_creatures(self, ix, cnxn, genus, species):
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
            creator = ix.get_by_id(cnxn, genus, i + 1)
            
            ix.add_creature(
                cnxn, genus, creator, 
                species, self.trait, f'{species}_{i}'
            )
        
        sql = f'SELECT * FROM {species};'
        creatures = ix.get_df(cnxn, sql)
        
        return creatures
    
    def make_species(self, ix, cnxn, genus, species_prefix):
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
        species : list(pandas.DataFrame)
            List of dataframes containing creature data.
    
        '''
        species = []
        
        for i in range(self.species_per_genus):
            species_name = f'{species_prefix}_{i}'
            species += [self.make_creatures(ix, cnxn, genus, species_name)]
            
        return species
        
    def make(self):
        '''
        Make test data.
        
        Makes a single creator table & 3 generations of 
        creature tables. Each generation has species_per_genus 
        tables, with num_beings creatures in each table. The 
        creators & creatures all have the attribute trait.
    
        Returns
        -------
        fathers : list(pandas.DataFrame)
            List containing a single dataframe of creator 
            data.
        sons : list(pandas.DataFrame)
            List containing species_per_genus dataframes 
            of creature data.
        grandsons : list(pandas.DataFrame)
            List containing (species_per_genus)^2 dataframes 
            of creature data.
        great_grandsons : list(pandas.DataFrame)
            List containing (species_per_genus)^3 dataframes 
            of creature data.

        '''
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            genus = 'creators'
            fathers = [self.make_creators(ix, cnxn, genus)]
            species_prefix = 'creatures'
            sons = self.make_species(ix, cnxn, genus, species_prefix)
            grandsons = []
            
            for i in range(self.species_per_genus):
                genus = f'creatures_{i}'
                species_prefix = f'creatures_{i}'
                
                grandsons += self. make_species(
                    ix, cnxn, genus, species_prefix
                )
                
            great_grandsons = []
            
            for i in range(self.species_per_genus):
                for j in range(self.species_per_genus):
                    genus = f'creatures_{i}_{j}'
                    species_prefix = f'creatures_{i}_{j}'
                    
                    great_grandsons += self.make_species(
                        ix, cnxn, genus, species_prefix
                    )
                                
        return fathers, sons, grandsons, great_grandsons
    
    def get(self):
        with Indexia(self.test_db) as ix:
            cnxn = ix.open_cnxn(ix.db)
            sql = 'SELECT * FROM creators;'
            fathers = [ix.get_df(cnxn, sql)]
            
            sons = []
            grandsons = []
            great_grandsons = []
            
            for i in range(self.species_per_genus):
                sql = f'SELECT * FROM creatures_{i};'
                sons += [ix.get_df(cnxn, sql)]
                
                for j in range(self.species_per_genus):
                    sql = f'SELECT * FROM creatures_{i}_{j};'
                    grandsons += [ix.get_df(cnxn, sql)]
                    
                    for k in range(self.species_per_genus):
                        sql = f'SELECT * FROM creatures_{i}_{j}_{k};'
                        great_grandsons += [ix.get_df(cnxn, sql)]
                        
            return fathers, sons, grandsons, great_grandsons