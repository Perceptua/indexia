# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30 13:51:32 2022

@author: aphorikles

Template for daily indexia operations.
"""

from indexia.indexia import indexia
from inquiry import opera
from schemata import xml



class drone:            
    def start_indexia(self):
        self.ix = indexia()
        self.cnxn = self.ix.open_cnxn(self.ix.db)
        
        return self.ix, self.cnxn
    
    def render_corpus(self, scribe):
        corpus = self.ix.get_corpus(self.cnxn, scribe)
        image = xml.render_image(corpus)
        xml.display_image(image)
        
        return corpus
    


# setup
worker = drone()
ix, cnxn = worker.start_indexia()

# scribe
pseudonym = 'aphorikles'
scribe = ix.get_scribes(cnxn, pseudonym)

# library
libronym = 'black_box'
library = ix.get_libraries(cnxn, scribe, libronym)

# card
created = '2022-02-17-18-20'
card = ix.add_card(cnxn, library, created)

# logonym
logonyms = [
    'Canadarago, Elsewhere',
    'Tales from Oldenbarnevelt',
    'ice',
    'sea',
]

for logonym in logonyms: 
    ix.add_logonym(cnxn, card, logonym)

# render
worker.render_corpus(scribe)

# miscellaneous/workspace
current, set_to = 'abc', 'def'
sql = f'''
    UPDATE
        logonyms
    SET
        logonym = '{set_to}'
    WHERE
        card_id = {card.id.values[0]}
        AND logonym = '{current}';
'''

card = ix.get_cards(cnxn, library, card.created.values[0])

# inspect
table = 'logonyms'
sql = open(f'data/queries/{table}.sql').read()
result = opera.get_df(cnxn, sql)

# reindex
old_ids = list(result.id.values)
new_ids = [i + 1 for i in result.index]

for i in range(len(old_ids)):
    ix.update(cnxn, table, ['id'], [new_ids[i]], ['id'], [old_ids[i]])

ix.update(cnxn, 'SQLITE_SEQUENCE', ['seq'], [result.shape[0]], ['name'], [table])

# teardown
ix.close_all_cnxns()
