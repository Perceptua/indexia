from indexia.inquiry import Inquiry, Tabula
import unittest as ut


class TestInquiry(ut.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tablename: str = 'users'
        cls.values: list[str] = [('user1'), ('user2'), ('user3')]
        
        cls.columns: dict[str, str] = {
            'uid': 'INT PRIMARY KEY',
            'username': 'VARCHAR(28)'
        }
        
    def testCreate(self) -> None:
        statement: str = Inquiry.create(self.tablename, self.columns)
        
        expected: str = ' '.join([
            'CREATE TABLE IF NOT EXISTS users',
            '(uid INT PRIMARY KEY,username VARCHAR(28))'
        ])
        
        self.assertEqual(statement, expected)
        
    def testInsert(self) -> None:        
        statement: str = Inquiry.insert(
            self.tablename, 
            [(i, f'user{i}') for i in range(1, 4)]
        )
        
        expected: str = ' '.join([
            'INSERT INTO users VALUES',
            "('1','user1'),('2','user2'),('3','user3')"
        ])
        
        self.assertEqual(statement, expected)
        
    def testSelect(self) -> None:
        statement: str = Inquiry.select(
            self.tablename, 
            ['uid'], 
            'WHERE uid > 1'
        )
        
        expected: str = 'SELECT uid FROM users WHERE uid > 1'
        self.assertEqual(statement, expected)
    
    def testDelete(self) -> None:
        statement: str = Inquiry.delete(self.tablename)
        expected: str = 'DELETE FROM users '
        self.assertEqual(statement, expected)
        
        statement = Inquiry.delete(
            self.tablename, 
            conditions="WHERE username = 'user1'"
        )
        
        expected = "DELETE FROM users WHERE username = 'user1'"
        self.assertEqual(statement, expected)
        
    def testUpdate(self) -> None:
        statement: str = Inquiry.update(
            self.tablename, 
            ['username'], 
            ['user4'],
            conditions="WHERE username = 'user1'"
        )
        
        expected: str = ' '.join([
            "UPDATE users SET username = 'user4'",
            "WHERE username = 'user1'"
        ])
        
        self.assertEqual(statement, expected)
    
    def testWhere(self) -> None:
        statement: str = Inquiry.where(
            ['username', 'username'], 
            ['user1', 'user2'],
            conjunction='OR'
        )
        
        expected: str = "WHERE username = 'user1' OR username = 'user2'"
        self.assertEqual(statement, expected)


class TestTabula(ut.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.genus: str = 'scribes'
        cls.species: str = 'libraries'
        cls.genus_trait: str = 'pseudonym'
        cls.species_trait: str = 'libronym'
        
    def testGetCreatorTable(self) -> None:
        creator_info: tuple[str, dict[str, str]] = Tabula.get_creator_table(
            self.genus, self.genus_trait
        )
        genus: str = creator_info[0]
        cols: dict[str, str] = creator_info[1] 
        
        self.assertEqual(self.genus, genus)
        self.assertEqual({'id', self.genus_trait}, set(cols.keys()))
        
    def testGetCreatureTable(self) -> None:
        creature_info: tuple[str, dict[str, str]] = Tabula.get_creature_table(
            self.genus, self.species, self.species_trait
        )

        species: str = creature_info[0]
        cols: dict[str, str] = creature_info[1]
        
        self.assertEqual(self.species, species)
        
        self.assertEqual({
            'id', 
            self.species_trait, 
            f'{self.genus}_id', 
            f'FOREIGN KEY ({self.genus}_id)'
        }, set(cols.keys()))
    
    def testReferences(self) -> None:
        references: str = Tabula.references(
            self.genus, 
            self.genus_trait
        )
        
        expected: str = ' '.join([
            f'REFERENCES {self.genus}({self.genus_trait})',
            'ON DELETE CASCADE ON UPDATE CASCADE'
        ])
        
        self.assertEqual(references, expected)


if __name__ == '__main__':
    ut.main()