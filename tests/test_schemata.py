# -*- coding: utf-8 -*-
"""
Created on Thu Dec 29 15:35:00 2022

@author: aphorikles
"""

from indexia import indexia
from schemata import xml as renderer
import os
import unittest as ut
import xml.etree.ElementTree as et



class TestXML(ut.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pseudonym = 'aphorikles'
        
        with indexia() as ix:
            cnxn = ix.open_cnxn(ix.db)
            cls.scribe = ix.add_scribe(cnxn, cls.pseudonym)
            cls.corpus = ix.get_corpus(cnxn, cls.scribe)
            
    def testRenderImage(self):
        image = renderer.render_image(self.corpus)
        self.assertIsInstance(image, et.ElementTree)
    
    def testDisplayImage(self):
        image = renderer.render_image(self.corpus)
        renderer.display_image(image)
        self.assertFalse(os.path.isfile('data/corpus.xml'))

if __name__ == '__main__':
    ut.main()