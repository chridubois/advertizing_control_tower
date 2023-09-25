{{ config(
  materialized = 'view'
) }}

WITH hubspot_deals AS (

  SELECT
    *,
    (
      CASE
        WHEN deal_stage IN (
          'Closed lost',
          'Quote signed',
          'Validated',
          'Quote sent',
          'Installation done'
        ) THEN 1
        ELSE 0
      END
    ) AS proposals,
    (
      CASE
        WHEN deal_stage IN (
          'Closed Won',
          'Quote signed',
          'Validated',
          'Installation done'
        ) THEN 1
        ELSE 0
      END
    ) AS closings
  FROM
    {{ ref('stg_hubspot_deals') }}
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
  deal_stage,
  SUM(
    deal_amount
  ) AS deal_amount,
  SUM(
    proposals
  ) AS proposals,
  SUM(
    closings
  ) AS closings
FROM
  hubspot_deals
GROUP BY
  DATE,
  MONTH,
  client,
  source,
  deal_stage
