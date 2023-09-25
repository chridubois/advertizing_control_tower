{{ config(
  materialized = 'view'
) }}

WITH hubspot_monthly AS (

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
  hubspot_monthly
GROUP BY
  MONTH,
  client,
  source
