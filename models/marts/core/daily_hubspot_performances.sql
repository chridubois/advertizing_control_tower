{{ config(
  materialized = 'view'
) }}

WITH hubspot_deals AS (

  SELECT
    *
  FROM
    {{ ref('stg_hubspot_contacts') }}
)
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
  account AS client,
  contact_utm_source AS source,
  COUNT(
    DISTINCT contact_id
  ) AS contacts,
  SUM(
    deal_amount
  ) AS deal_amount,
  SUM(
    deal
  ) AS deals,
  SUM(
    quote_sent
  ) AS quote_sent,
  SUM(
    quote_signed
  ) AS quote_signed
FROM
  hubspot_deals
GROUP BY
  DATE,
  MONTH,
  client,
  source
