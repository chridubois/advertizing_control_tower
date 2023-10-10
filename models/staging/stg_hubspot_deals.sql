{{ config(
  materialized = 'view'
) }}

WITH deal AS (

  SELECT
    *
  FROM
    {{ ref('stg_hubspot_deal_union') }}
),
deal_pipeline AS (
  SELECT
    *
  FROM
    {{ ref('stg_hubspot_deal_pipeline_union') }}
),
deal_pipeline_stage AS (
  SELECT
    *
  FROM
    {{ ref('stg_hubspot_deal_pipeline_stage_union') }}
),
deal_contact AS (
  SELECT
    *
  FROM
    {{ ref('stg_hubspot_deal_contact_union') }}
),
contact AS (
  SELECT
    *
  FROM
    {{ ref('stg_hubspot_contact_union') }}
)
SELECT
  C.account,
  c.property_createdate AS date,
  d.deal_id AS deal_id,
  dp.label AS deal_pipeline,
  dps.label AS deal_stage,
  d.property_amount AS deal_amount,
  c.id AS contact_id,
  c.property_utm_source AS contact_utm_source,
  c.property_utm_medium AS contact_utm_medium,
  c.property_utm_campaign AS contact_utm_campaign,
FROM
  deal d
JOIN deal_pipeline dp
  ON dp.pipeline_id = d.deal_pipeline_id
JOIN deal_pipeline_stage dps
  ON dps.stage_id = d.deal_pipeline_stage_id
JOIN deal_contact dc
  ON d.deal_id = dc.deal_id
JOIN contact c
  ON dc.contact_id = c.id
