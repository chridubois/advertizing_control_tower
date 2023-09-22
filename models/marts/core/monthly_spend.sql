{{ config(
  materialized = 'view'
) }}

WITH google_ads_spend AS (

  SELECT
    CONCAT(EXTRACT(
      YEAR
      FROM
        date_day
    ), '-', EXTRACT(
      MONTH
      FROM
        date_day
    )) AS MONTH,
    client,
    'google_ads' AS source,
    SUM(
       spend
    ) AS spend,
    SUM(
       clicks
    ) AS clicks,
    SUM(
       impressions
    ) AS impressions
  FROM
    {{ ref('stg_google_ads_campaigns') }}
  GROUP BY
    MONTH,
    client,
    source
)
SELECT
  *
FROM
  google_ads_spend
