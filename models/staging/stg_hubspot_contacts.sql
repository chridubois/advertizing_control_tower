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
  C.property_createdate AS DATE,
  d.deal_id AS deal_id,
  (
    CASE
      WHEN d.deal_id IS NOT NULL THEN 1
      ELSE 0
    END
  ) AS deal,
  (
    CASE
      WHEN dps.label IN (
        'Quote signed',
        'Validated',
        'Quote sent',
        'Installation done'
      ) THEN 1
      ELSE 0
    END
  ) AS quote_sent,
  (
    CASE
      WHEN dps.label IN (
        'Closed Won',
        'Quote signed',
        'Validated',
        'Installation done'
      ) THEN 1
      ELSE 0
    END
  ) AS quote_signed,
  (
    IF(dps.label IN ('Quote sent')
    AND d.property_hs_deal_stage_probability >= 0.8, 1, 0)) AS quote_pending_high_probability,
    dp.label AS deal_pipeline,
    dps.label AS deal_stage,
    d.property_amount AS deal_amount,
    C.id AS contact_id,
    (
      CASE
        WHEN C.property_utm_source IS NULL
        OR C.property_utm_source = ''
        OR C.property_utm_source = 'ig' THEN 'autre'
        WHEN C.property_utm_source = 'fb' THEN 'facebook'
        ELSE C.property_utm_source
      END
    ) AS contact_utm_source,
    C.property_utm_medium AS contact_utm_medium,
    C.property_utm_campaign AS contact_utm_campaign,
    FROM
      contact C
      LEFT JOIN deal_contact dc
      ON dc.contact_id = C.id
      LEFT JOIN deal d
      ON d.deal_id = dc.deal_id
      LEFT JOIN deal_pipeline dp
      ON dp.pipeline_id = d.deal_pipeline_id
      LEFT JOIN deal_pipeline_stage dps
      ON dps.stage_id = d.deal_pipeline_stage_id
