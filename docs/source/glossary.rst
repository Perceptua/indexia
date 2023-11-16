Glossary
--------

being
  A record in an ``indexia`` table. May be a creator or a creature.

creator
  A record in a genus (parent) table. Creators may have one or more creatures 
  in species (child) tables. All creators have an ``id`` & one text trait.
  
creature
  A record in a species (child) table. Creatures must have one & only one 
  creator from a genus (parent) table. All creatures have an ``id``, a text 
  trait, & one foreign key relationship on the genus of their creator.
  
corpus
  From Latin, meaning body. A dataframe representation of ``indexia`` data.

dendron
  From Ancient Greek, meaning tree. An XML element tree representation of 
  ``indexia`` data.
  
diktua
  From Ancient Greek, meaning net. A network graph representation of 
  ``indexia`` data.
  
eidola
  From Ancient Greek, meaning figures or representations. Module-generated 
  sample data or tables.
  
genus
  A parent table containing creator records. Columns are ``id`` & ``<trait>``, 
  where ``<trait>`` is a user-defined attribute of the creators, stored in the 
  table as text. Changes to the genus or its creators cascade to creatures.
  
id
  Unique identifier of a creator or creature. The column ``id`` is the primary 
  key of the genus or species table. Must not be null.
  
kind
  Name of an ``indexia`` table. May be a genus or species.
  
species
  A child table containing creature records. Columns are ``id``, ``<trait>``, &
  ``<genus_id>``, where ``<trait>`` is a user-defined attribute of the 
  creatures & ``<genus_id>`` represents the ``id`` of a creature's creator.
  
trait
  Attribute of a creator or creature, stored as text. Must not be null.