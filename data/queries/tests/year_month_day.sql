SELECT
  *
FROM
  cards
WHERE
  DATEPART(year, created) = '2022'
  AND DATEPART(month, created) = '11'
  AND DATEPART(day, created) = '5'
