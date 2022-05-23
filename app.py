# -*- coding: utf-8 -*-
"""
Created on Sun May 22 12:52:36 2022

@author: aphorikles
"""

from flask import Flask, render_template, request
from indexia import indexia
from terminal import terminal

app = Flask(__name__)

@app.route('/', methods=['POST', 'GET'])
@app.route('/<pseudonym>', methods=['GET'])
def libraries(pseudonym=None):
    title = terminal.read('data/indexia_lower.txt')
    libraries = None
    
    if request.method == 'POST':
        pseudonym = request.form['pseudonym']
    
    if pseudonym:
        with indexia() as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, pseudonym)
            libraries = ix.get_libraries(cnxn, scribe)
            libraries = None if libraries.empty else list(libraries.libronym)
    
    return render_template('libraries.html', 
                           title=title, 
                           pseudonym=pseudonym, 
                           libraries=libraries)

@app.route('/<pseudonym>/create', methods=['POST'])
@app.route('/<pseudonym>/<libronym>', methods=['GET', 'POST'])
def cards(pseudonym, libronym=None):
    title = terminal.read('data/indexia_lower.txt')
    cards = None
    
    if request.method == 'POST':
        libronym = request.form['libronym']
    
    if libronym:
        with indexia() as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, pseudonym)
            library = ix.add_library(cnxn, libronym, scribe)
            cards = ix.get_cards(cnxn, library)
            cards = None if cards.empty else list(cards.created)
            
    return render_template('cards.html',
                           title=title,
                           pseudonym=pseudonym,
                           libronym=libronym,
                           cards=cards)


@app.route('/<pseudonym>/<libronym>/create', methods=['POST'])
@app.route('/<pseudonym>/<libronym>/<created>', methods=['GET', 'POST'])
def logonyms(pseudonym, libronym, created=None):
    title = terminal.read('data/indexia_lower.txt')
    logonyms = []
    
    if request.method == 'POST':
        created = request.form['created']
        logonyms = request.form['logonyms'].split(' ')
        
    if created:
        with indexia() as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, pseudonym)
            library = ix.add_library(cnxn, libronym, scribe)
            card = ix.add_card(cnxn, library, created)
            
            for logonym in logonyms:
                ix.add_logonym(cnxn, logonym, library, card)
                
            logonyms = ix.get_logonyms(cnxn, card)
            logonyms = None if logonyms.empty else list(logonyms.logonym)
        
    return render_template('logonyms.html',
                           title=title,
                           pseudonym=pseudonym,
                           libronym=libronym,
                           created=created,
                           logonyms=logonyms)