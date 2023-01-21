SELECT
  *
FROM
  cards
WHERE
  DATEPART({date_part}, created) = '{value}'
