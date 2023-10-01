{{ config(
  materialized = 'view'
) }}

WITH taboola_ads AS (

  SELECT
    *
  FROM
    {{ ref('stg_taboola_ads_campaigns') }}
)
SELECT
  *
FROM
  taboola_ads
