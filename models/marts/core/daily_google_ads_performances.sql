{{ config(
  materialized = 'view'
) }}

WITH google_ads_perfs AS (

  SELECT
    CAST(
      CONCAT(
        EXTRACT(
          YEAR
          FROM
            DATE
        ),
        '-',
        EXTRACT(
          MONTH
          FROM
            DATE
        ),
        '-',
        EXTRACT(
          DAY
          FROM
            DATE
        )
      ) AS DATE
    ) AS DATE,
    CONCAT(
      EXTRACT(
        YEAR
        FROM
          DATE
      ),
      '-',
      EXTRACT(
        MONTH
        FROM
          DATE
      )
    ) AS MONTH,
    customer_id,
    'google_ads' AS source,
    SUM(
      cost_micros
    ) AS spend,
    SUM(
      clicks
    ) AS clicks,
    SUM(
      impressions
    ) AS impressions,
    SUM(
      conversions
    ) AS conversions,
    SUM(
      conversions_value
    ) AS conversions_value,
  FROM
    {{ ref('stg_google_ads_campaigns_stats') }}
  GROUP BY
    date,
    MONTH,
    customer_id,
    source
),
clients AS (
  SELECT
    *
  FROM
    {{ ref('clients') }}
)
SELECT
  google_ads_perfs.date,
  google_ads_perfs.month,
  clients.clientName AS client,
  google_ads_perfs.source,
  google_ads_perfs.spend / 1000000 AS spend,
  google_ads_perfs.clicks,
  google_ads_perfs.impressions,
  google_ads_perfs.conversions,
  google_ads_perfs.conversions_value
FROM
  google_ads_perfs
  LEFT JOIN clients
  ON google_ads_perfs.customer_id = clients.accountId
  AND clients.platform IN ('google_ads', 'google')
