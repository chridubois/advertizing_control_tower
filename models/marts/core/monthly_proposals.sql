{{ config(
  materialized = 'view'
) }}

WITH hubspot_deals AS (

  SELECT
    CONCAT(EXTRACT(
      YEAR
      FROM
        contact_createdate
    ), '-', EXTRACT(
      MONTH
      FROM
        contact_createdate
    )) AS MONTH,
    account AS client,
    contact_utm_source AS source,
    COUNT(
      DISTINCT deal_id
    ) AS proposals
  FROM
    {{ ref('stg_hubspot_deals') }}
  WHERE
    deal_stage IN (
      'Closed lost',
      'Quote signed',
      'Validated',
      'Quote sent',
      'Installation done'
    )
  GROUP BY
    MONTH,
    client,
    source
)
SELECT
  *
FROM
  hubspot_deals
