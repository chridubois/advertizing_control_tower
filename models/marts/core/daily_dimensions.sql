{{ config(
  materialized = 'view'
) }}

WITH clients AS (

  SELECT clientName AS client,
  platform AS source
  FROM
    {{ ref('clients') }}
), dates AS (
  SELECT date, month
  FROM
    {{ ref('dates') }}
)
SELECT DISTINCT *
FROM clients
CROSS JOIN dates
