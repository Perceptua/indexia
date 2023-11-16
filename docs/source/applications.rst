Applications
============

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
definitions & SQL operations, see ``inquiry`` in `Reference <modules.html>`_.