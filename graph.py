# -*- coding: utf-8 -*-
"""
Created on Sun Jul 24 12:05:52 2022

@author: aphorikles
"""

from plotly import graph_objects as go
import itertools
import networkx as nx



class logograph:
    def __init__(self, logonyms):
        self.logonyms = logonyms
    
    def get_edge_list(self):
        ids_by_logonym = {logonym: [] \
                          for logonym in self.logonyms.logonym.unique()}
        
        for i, data in self.logonyms.iterrows():
            ids_by_logonym[data['logonym']] += [data['card_id']]
        
        edge_list = [list(itertools.combinations(ids_by_logonym[i], 2)) \
                     for i in ids_by_logonym]
        
        return edge_list