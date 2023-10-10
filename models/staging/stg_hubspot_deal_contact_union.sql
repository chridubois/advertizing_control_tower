{{ config(
  materialized = 'view'
) }}

WITH ensol AS (

  SELECT
    *
  FROM
    {{ source(
      'hubspot_ensol',
      'deal_contact'
    ) }}
),
eversun AS (
  SELECT
    *
  FROM
    {{ source(
      'hubspot_eversun',
      'deal_contact'
    ) }}
)
SELECT
  'ensol' AS account,*
FROM
  ensol
UNION ALL
SELECT
  'eversun' AS account,*
FROM
  eversun
