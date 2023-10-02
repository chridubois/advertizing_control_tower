{{ config(
  materialized = 'view'
) }}

WITH clients AS (

  SELECT clientName AS client,
  platform AS source
  FROM
    {{ ref('clients') }}
), dates AS (
  SELECT month_of_the_year
  FROM
    {{ ref('months') }}
)
SELECT DISTINCT *
FROM clients
CROSS JOIN dates
