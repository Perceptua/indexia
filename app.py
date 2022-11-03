# -*- coding: utf-8 -*-
"""
Created on Sun May 22 12:52:36 2022

@author: aphorikles
"""

from datetime import datetime as dt
from flask import Flask, redirect, render_template, request, url_for
from indexia import indexia
from schemata import tabula
from queries import inquiry
from terminal import terminal

app = Flask(__name__)



def add_counts(cnxn, libraries):
    for i, library in libraries.copy().iterrows():
        cards, logonyms = inquiry.count_library_items(cnxn, library.id)
        libraries.loc[i, 'card_count'] = str(cards)
        libraries.loc[i, 'logonym_count'] = str(logonyms)
    
    return libraries

@app.route('/', methods=['POST', 'GET'])
@app.route('/<pseudonym>', methods=['GET'])
def libraries(pseudonym=None):
    title = terminal.read('data/indexia_lower.txt')
    libraries = None
    
    if request.method == 'POST':
        pseudonym = request.form['pseudonym']
    
    if pseudonym and pseudonym != 'favicon.ico':
        with indexia() as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, pseudonym)
            libraries = ix.get_libraries(cnxn, scribe).sort_values(by='libronym')
            get_last_updated = lambda i: inquiry.library_last_updated(cnxn, i)
            
            if libraries.empty:
                libraries = None
            else:
                libraries['last_updated'] = libraries.id.apply(get_last_updated)
                libraries = add_counts(cnxn, libraries)
                libraries = libraries.to_dict(orient='records')
    
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
            cards = ix.get_cards(cnxn, library).sort_values(by='created')
            preview = lambda i: inquiry.card_preview(cnxn, i)
            
            if cards.empty:
                cards = None
            else:
                cards['preview'] = cards.id.apply(preview)
                cards = cards.to_dict(orient='records')
        
    return render_template('cards.html',
                           title=title,
                           pseudonym=pseudonym,
                           libronym=libronym,
                           cards=cards)

def check_created_format(created):
    try:
        dt.strptime(created, '%Y-%m-%d-%H-%M')
        return True
    except:
        return False
    
@app.route('/<pseudonym>/<libronym>/create', methods=['GET', 'POST'])
def create_card(pseudonym, libronym, created=None):
    if request.method == 'POST':
        created = request.form['created']
    
    if check_created_format(created):
        with indexia() as ix:
            cnxn = ix.open_cnxn(ix.db)
            scribe = ix.add_scribe(cnxn, pseudonym)
            library = ix.add_library(cnxn, libronym, scribe)
            ix.add_card(cnxn, library, created)
    
    return redirect(url_for('.cards', pseudonym=pseudonym, libronym=libronym))

@app.route('/<pseudonym>/<libronym>/<created>', methods=['GET', 'POST'])
def logonyms(pseudonym, libronym, created):
    title = terminal.read('data/indexia_lower.txt')
    logonyms = []
    
    if request.method == 'POST':
        logonyms = request.form['logonyms'].split(' ')
    
    with indexia() as ix:
        cnxn = ix.open_cnxn(ix.db)
        scribe = ix.add_scribe(cnxn, pseudonym)
        library = ix.add_library(cnxn, libronym, scribe)
        card = ix.add_card(cnxn, library, created)
        
        for logonym in logonyms:
            ix.add_logonym(cnxn, logonym, card)
            
        logonyms = ix.get_logonyms(cnxn, card).sort_values(by='logonym')
        logonyms = None if logonyms.empty else logonyms.to_dict(orient='records')

        
    return render_template('logonyms.html',
                           title=title,
                           pseudonym=pseudonym,
                           libronym=libronym,
                           created=created,
                           logonyms=logonyms)

@app.route('/update/<tablename>/<object_id>', methods=['GET', 'POST'])
def update(tablename, object_id):
    title = terminal.read('data/indexia_lower.txt')
    set_cols = [k for k in tabula().columns[tablename].keys() if k != 'id']
    
    with indexia() as ix:
        cnxn = ix.open_cnxn(ix.db)
    
        if request.method == 'POST':
            redirect_url = request.form['url'].split('/')
            redirect_object, values = redirect_url[0], redirect_url[1:]
            set_values = [request.form[col] for col in set_cols]
            where_cols, where_values = ['id'], [object_id]
            ix.update(cnxn, tablename, set_cols, set_values, where_cols, where_values)
        
        return redirect(url_for(f'.{redirect_object}', *values))
    
    return render_template('update.html',
                           title=title,
                           object_id=object_id,
                           set_cols=set_cols)
            

@app.route('/delete/<tablename>/<object_id>', methods=['POST'])
def delete(tablename, object_id):
    redirect_url = request.form['url']
    
    with indexia() as ix:
        cnxn = ix.open_cnxn(ix.db)
        ix.delete(cnxn, tablename, object_id)
    
    return redirect(redirect_url)