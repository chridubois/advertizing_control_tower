{{ config(
  materialized = 'view'
) }}

WITH ensol AS (

  SELECT
    *
  FROM
    {{ source(
      'hubspot_ensol',
      'deal_pipeline_stage'
    ) }}
),
eversun AS (
  SELECT
    *
  FROM
    {{ source(
      'hubspot_eversun',
      'deal_pipeline_stage'
    ) }}
)
SELECT
  'ensol' AS account,
  label,
  stage_id
FROM
  ensol
UNION ALL
SELECT
  'eversun' AS account,
  label,
  CAST(stage_id AS STRING) AS stage_id
FROM
  eversun
