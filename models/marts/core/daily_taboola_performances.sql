{{ config(
  materialized = 'view'
) }}

WITH taboola_ads AS (

  SELECT
    date_time,
    client,
    source,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(spent) AS spend
  FROM
    {{ ref('stg_taboola_ads_campaigns') }}
  GROUP BY
    date_time,
    client,
    source
)
SELECT
  *
FROM
  taboola_ads
