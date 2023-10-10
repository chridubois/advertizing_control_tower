{{ config(
  materialized = 'view'
) }}

WITH ensol AS (

  SELECT
    *
  FROM
    {{ source(
      'hubspot_ensol',
      'deal_pipeline'
    ) }}
),
eversun AS (
  SELECT
    *
  FROM
    {{ source(
      'hubspot_eversun',
      'deal_pipeline'
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
