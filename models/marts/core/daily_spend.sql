{{ config(
  materialized = 'view'
) }}

WITH google_ads_spend AS (

  SELECT
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
        EXTRACT(
          DAY
          FROM
            date_day
        )
      ) AS DATE
    ) AS DATE,
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
      )
    ) AS MONTH,
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
    DATE,
    MONTH,
    client,
    source
),
facebook_ads_spend AS (
  SELECT
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
        EXTRACT(
          DAY
          FROM
            date_day
        )
      ) AS DATE
    ) AS DATE,
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
      )
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
ORDER BY
  DATE,
  MONTH,
  client,
  source
