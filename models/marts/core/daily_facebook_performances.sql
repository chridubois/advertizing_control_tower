{{ config(
  materialized = 'view'
) }}

WITH facebook_ads_spend AS (

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
    account_id,
    account_name,
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
    account_id,
    account_name,
    source
)
SELECT
  *
FROM
  facebook_ads_spend
