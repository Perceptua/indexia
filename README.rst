README
======

Collections & connections. A virtual Zettelkasten.

Installation
------------

``pip install indexia``

Usage
-----

Full documentation available at `Read the Docs <https://indexia.readthedocs.io/en/latest/>`_. The example below uses an ``indexia`` template to generate sample tables & data.

.. code-block:: python

    from indexia.eidola import Templates

    db = 'test.db'
    objects = Templates(db).build_template('philosophy')
    
Update & manage ``indexia`` data with ``indexia.indexia.Indexia``:

.. code-block:: python

    from indexia.indexia import Indexia
    
    philosophers = objects['philosophers']
    aristotle = philosphers[philosphers.name == 'Aristotle']

    with Indexia(db) as ix:
        cnxn = ix.open_cnxn(ix.db)
        
        on_dreams = ix.add_creature(
            cnxn, 'philosophers', 
            aristotle, 'works', 
            'title', 'On Dreams'
        )
        
        dreams = ix.add_creature(
            cnxn, 'works', 
            on_dreams, 'topics', 
            'name', 'dreams'
        )
        
Render sample data as an XML tree with ``indexia.schemata.Dendron``:

.. code-block:: python

    from indexia.schemata import Dendron
    
    dendron = Dendron(db)
    image = dendron.render_image('philosophers', philosphers)
    dendron.write_image(image, open_browser=True)
    
Build dataframe of sample data with ``indexia.schemata.Corpus``:

.. code-block:: python

    from indexia.schemata import Corpus
    
    corpus = Corpus(db, 'philosophers', philosphers).assemble()

Render sample data as a network graph with ``indexia.schemata.Diktua``:

.. code-block:: python

    from indexia.schemata import Diktua
    
    topics = corpus[corpus.species == 'topics']
    diktua = Diktua(topics, 'expression', 'creator_id')
    diktua.style_nodes()
    diktua.plot(plot_path='diktua.html')