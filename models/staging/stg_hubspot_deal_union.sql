{{ config(
  materialized = 'view'
) }}

WITH ensol AS (

  SELECT
    *
  FROM
    {{ source(
      'hubspot_ensol',
      'deal'
    ) }}
),
eversun AS (
  SELECT
    *
  FROM
    {{ source(
      'hubspot_eversun',
      'deal'
    ) }}
)
SELECT
  'ensol' AS account,
  deal_id,
  deal_pipeline_id,
  deal_pipeline_stage_id,
  property_hs_deal_stage_probability,
  property_amount
FROM
  ensol
UNION ALL
SELECT
  'eversun' AS account,
  deal_id,
  deal_pipeline_id,
  CAST(deal_pipeline_stage_id AS STRING) AS deal_pipeline_stage_id,
  property_hs_deal_stage_probability,
  0.0 AS property_amount
FROM
  eversun
