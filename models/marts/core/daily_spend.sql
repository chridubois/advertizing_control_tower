{{ config(
  materialized = 'view'
) }}

WITH google_ads_spend AS (

  SELECT
    DATE,
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
        '01'
      ) AS DATE
    ) AS MONTH,
    client,
    (
      CASE
        WHEN client = 'ensol' THEN 'google'
        ELSE 'google_ads'
      END
    ) AS source,
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
    {{ ref('daily_google_ads_performances') }}
  GROUP BY
    DATE,
    MONTH,
    client,
    source
),
facebook_ads_spend AS (
  SELECT
    date_day AS DATE,
    CAST(
      CONCAT(
        EXTRACT(
          YEAR
          FROM
            date_day
        ),
        '-',
        EXTRACT(
          MONTH
          FROM
            date_day
        ),
        '-',
        '01'
      ) AS DATE
    ) AS MONTH,
    account_name AS client,
    'facebook' AS source,
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
    {{ ref('stg_facebook_ads_campaigns') }}
  GROUP BY
    DATE,
    MONTH,
    client,
    source
),
taboola_ads_spend AS (
  SELECT
    CAST(
      CONCAT(
        EXTRACT(
          YEAR
          FROM
            date_time
        ),
        '-',
        EXTRACT(
          MONTH
          FROM
            date_time
        ),
        '-',
        EXTRACT(
          DAY
          FROM
            date_time
        )
      ) AS DATE
    ) AS DATE,
    CAST(
      CONCAT(
        EXTRACT(
          YEAR
          FROM
            date_time
        ),
        '-',
        EXTRACT(
          MONTH
          FROM
            date_time
        ),
        '-',
        '01'
      ) AS DATE
    ) AS MONTH,
    client,
    source,
    SUM(
      spent
    ) AS spend,
    SUM(
      clicks
    ) AS clicks,
    SUM(
      impressions
    ) AS impressions
  FROM
    {{ ref('stg_taboola_ads_campaigns') }}
  GROUP BY
    DATE,
    MONTH,
    client,
    source
),
autres_platform_spend AS (
  SELECT
    CAST(
      MONTH AS DATE
    ) AS DATE,
    CAST(
      MONTH AS DATE
    ) AS MONTH,
    'ensol' AS client,
    (
      CASE
        WHEN platform IN (
          'France leads',
          'france leads'
        ) THEN 'France Leads'
        ELSE platform
      END
    ) AS source,
    spend,
    0 AS clicks,
    0 AS impressions
  FROM
    {{ source(
      'google_sheets_spend_bricks',
      'google_sheets_spend_ensol'
    ) }}
)
SELECT
  *
FROM
  google_ads_spend
UNION ALL
SELECT
  *
FROM
  facebook_ads_spend
UNION ALL
SELECT
  *
FROM
  taboola_ads_spend
UNION ALL
SELECT
  *
FROM
  autres_platform_spend
ORDER BY
  DATE,
  MONTH,
  client,
  source
