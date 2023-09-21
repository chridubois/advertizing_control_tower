{{ config(
  materialized = 'view'
) }}

WITH hubspot_deals AS (

  SELECT
    EXTRACT(
      MONTH
      FROM
        contact_createdate
    ) AS MONTH,
    account AS client,
    contact_utm_source AS source
  FROM
    {{ ref('stg_hubspot_deals') }}
  GROUP BY
    MONTH,
    client,
    source
)
SELECT
  *
FROM
  hubspot_deals
