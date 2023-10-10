{{ config(
  materialized = 'view'
) }}

WITH ensol AS (

  SELECT
    *
  FROM
    {{ source(
      'hubspot_ensol',
      'contact'
    ) }}
),
eversun AS (
  SELECT
    *
  FROM
    {{ source(
      'hubspot_eversun',
      'contact'
    ) }}
)
SELECT
  'ensol' AS account,
  property_createdate,
  id,
  property_utm_source,
  property_utm_medium,
  property_utm_campaign
FROM
  ensol
UNION ALL
SELECT
  'eversun' AS account,
  property_createdate,
  id,
  property_utm_source,
  property_utm_medium,
  property_utm_campaign
FROM
  eversun
