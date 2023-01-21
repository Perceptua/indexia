SELECT
  libraries.id AS library_id,
  libraries.libronym,
  cards.id AS card_id,
  cards.created,
  logonyms.id AS logonym_id,
  logonyms.logonym
FROM
  libraries libraries
LEFT JOIN
  cards cards
ON
  libraries.id = cards.library_id
LEFT JOIN
  logonyms logonyms
ON
  cards.id = logonyms.card_id
WHERE
  scribe_id = {scribe_id}
ORDER BY
  cards.library_id,
  logonyms.card_id,
  logonyms.id;
