indexia
=======

Collections & connections. A virtual Zettelkasten.

For source documentation, see :doc:`modules`. For definitions of ``indexia`` 
terms, see the :doc:`glossary`. To learn how ``indexia`` can be used, see 
`Usage`_ & `Applications`_ below.

Installation
------------

``pip install indexia``

Usage
-----

This example employs a template to generate sample data:

.. code-block:: python

    from indexia.eidola import Templates
    
    db = 'test.db'
    generator = Templates(db)
    generator.show_templates()
    objects = generator.build_template('philosophy')
    
The ``build_template`` method of ``eidola.Templates`` creates & populates 
several database tables with hierarchical relationships. The hierarchy of 
tables in ``'philosophy'`` & other available templates is illustrated by the 
``show_templates`` method. To generate tables & data from scratch, use the 
``add_creator`` & ``add_creature`` methods of ``indexia.Indexia``:

.. code-block:: python
    
    from indexia.indexia import Indexia
    
    with Indexia(db) as ix:
        cnxn = ix.open_cnxn(ix.db)
        
        homer = ix.add_creator(
            cnxn=cnxn, 
            genus='poets', 
            trait='name', 
            expr='Homer'
        )
        
        iliad = ix.add_creature(
            cnxn=cnxn, 
            genus='poets', 
            creator=homer, 
            species='epics', 
            trait='title', 
            expr='The Iliad'
        )

Once objects have been generated, they can be updated & managed with 
``indexia.Indexia``:

.. code-block:: python

    from indexia.indexia import Indexia
    
    philosophers = objects['philosophers']
    aristotle = philosophers[philosophers.name == 'Aristotle']
    
    with Indexia(db) as ix:
        cnxn = ix.open_cnxn(ix.db)
    
        works_of_aristotle = ix.get_creatures(
            cnxn=cnxn,
            genus='philosophers',
            creator=aristotle
        )
        
        table, data = works_of_aristotle[0]
        on_the_soul = data[data.title == 'On the Soul']
        work_id = on_the_soul.id.values[0]
        
        works_deleted = ix.delete(
            cnxn=cnxn,
            species=table,
            entity_id=work_id
        )
        
        assert(works_deleted == 1)

        on_dreams = ix.add_creature(
            cnxn=cnxn, 
            genus='philosophers',
            creator=aristotle, 
            species='works',
            trait='title', 
            expr='On Dreams'
        )

        dreams = ix.add_creature(
            cnxn=cnxn, 
            genus='works',
            creator=on_dreams, 
            species='topics',
            trait='name', 
            expr='dreams'
        )
    
To view the full data hierarchy under a given set of creator entities, create 
a dataframe using ``schemata.Corpus``:

.. code-block:: python

    from indexia.schemata import Corpus
    
    corpus = Corpus(
        db=db, 
        genus='philosophers', 
        creators=philosophers
    ).assemble()
    
Relationships between ``indexia`` objects can be visualized as a network graph
using ``schemata.Diktua``:

.. code-block:: python

    from indexia.schemata import Diktua
    
    works = corpus[corpus.species == 'works']
    
    diktua = Diktua(
        corpus=works, 
        as_nodes='expression', # nodes represent work titles
        as_edges='creator_id'  # edges represent shared authorship 
    )
    
    diktua.style_nodes()
    diktua.plot(plot_path='diktua.html')
    
Open ``diktua.html`` in a web browser to load the interactive network graph.

.. image:: _static/diktuagraph.png
  :width: 500
  :alt: Network graph of works by each philosopher in the template.
  
**Note:** ``schemata.Diktua`` uses ``pyvis`` for plotting, which can be slow 
for large or well-connected graphs. Performance improvements may be made in 
future releases.
    
``indexia`` data can also be represented as XML using ``schemata.Dendron``:

.. code-block:: python

    from indexia.schemata import Dendron
    
    dendron = Dendron(db)
    
    image = dendron.render_image(
        genus='philosophers', 
        creators=philosophers
    )
    
    dendron.write_image(
        image, 
        file_path='dendron.xml', 
        open_browser=True
    )
    
The ``render_image`` method of ``schemata.Dendron`` creates an XML tree of 
``indexia`` data, which can be saved & opened in a browser window with 
``write_image``. The tree created for the ``'philosophy'`` template data 
looks like this:

.. code-block:: xml
    
    <root>
        <philosophers id="1" name="Plato">
            <works id="1" title="Apology of Socrates" philosophers_id="1">
                <topics id="1" name="civics" works_id="1"/>
            </works>
            <works id="2" title="Symposium" philosophers_id="1">
                <topics id="2" name="love" works_id="2"/>
            </works>
            <works id="3" title="Republic" philosophers_id="1">
                <topics id="3" name="civics" works_id="3"/>
            </works>
        </philosophers>
        <philosophers id="2" name="Aristotle">
            <works id="4" title="On the Heavens" philosophers_id="2">
                <topics id="4" name="cosmology" works_id="4"/>
            </works>
            <works id="5" title="Topics" philosophers_id="2">
                <topics id="5" name="logic" works_id="5"/>
            </works>
            <works id="7" title="On Dreams" philosophers_id="2">
                <topics id="7" name="dreams" works_id="7"/>
            </works>
        </philosophers> 
    </root>   


Applications
------------

``indexia`` was originally designed for projects which employ the 
`Zettelkasten <https://en.wikipedia.org/wiki/Zettelkasten>`_, or "slip box", 
method of notetaking. A template data structure for these projects is available 
through ``eidola.Templates``:

.. code-block:: python

    from indexia.eidola import Templates

    db = 'test.db'
    generator = Templates(db)
    objects = generator.build_template('zettelkasten')
    
The tables in this template are designed to answer questions about the project:

* ``scribes``: *Who?* Which member of the project created the document?

* ``libraries``: *Where?* Where is the document stored?

* ``cards``: *When?* When was the document created?

  * The order of documents can be determined relatively if the project uses 
    alphanumeric IDs, or absolutely if it uses datetime IDs.
    
* ``keywords``: *What?* What information does the document contain?

This out-of-the-box Zettelkasten is a useful application of ``indexia``, but it 
is not the only one. The ``'zettelkasten'`` template is only one example of a 
general, hierarchical data model employed by the ``indexia`` package. Another 
application of this model, cataloging philosophers & their works, can be seen 
in `Usage`_ above.

In general, ``indexia`` is well suited to any project involving 
`hierarchical data <https://en.wikipedia.org/wiki/Hierarchical_database_model>`_ 
or `tree structures <https://en.wikipedia.org/wiki/Tree_structure>`_. These 
data structures can be used to model

* Parent-child relationships

* Object-attribute relationships

* Sequential processes or decision trees

In addition to creating & managing data for these applications, ``indexia`` 
helps with generating graphs & representations of hierarchical data. The 
``Corpus``, ``Dendron``, & ``Diktua`` classes of ``indexia.schemata`` display 
hierarchical data as dataframes, XML trees, & network graphs, respectively.

Note on ``indexia`` data
------------------------

The ``indexia`` data model is easy to use & highly extensible, but note that 
it is very restrictive. Currently, the classes of ``indexia.schemata``, which 
render & display data, expect the following to hold true of all tables:

* There is a primary key column named ``id``

* There is one & only one attribute field (stored as type ``TEXT``)

* ``creator`` tables have no foreign key relationships

* ``creature`` tables have one & only one foreign key relationship 

Future releases may allow for greater flexibility. Also note that although the 
relationships between ``indexia`` tables are hierarchical in nature, the 
implementation uses foreign keys in a ``sqlite`` database (i.e., ``indexia`` is
not a pure implementation of the hierarchical database model). For table 
definitions & SQL operations, see ``inquiry`` in :doc:`modules`.