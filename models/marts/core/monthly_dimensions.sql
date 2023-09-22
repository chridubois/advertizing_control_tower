{{ config(
  materialized = 'view'
) }}

WITH clients AS (

  SELECT clientName AS client,
  platform AS source
  FROM
    {{ ref('clients') }}
), months AS (
  SELECT month_of_the_year AS month
  FROM
    {{ ref('months') }}
)
SELECT DISTINCT *
FROM clients
CROSS JOIN months
