{{ config(
  materialized = 'view'
) }}

WITH dimensions AS (

  SELECT
    *
  FROM
    {{ ref('daily_dimensions') }}
),
spend AS (
  SELECT
    *
  FROM
    {{ ref('daily_spend') }}
),
daily_hubspot_performances AS (
  SELECT
    *
  FROM
    {{ ref('daily_hubspot_performances') }}
),
clients AS (
  SELECT
    *
  FROM
    {{ ref('clients') }}
)
SELECT
  dimensions.date,
  dimensions.month,
  dimensions.client,
  dimensions.source,
  spend.impressions,
  spend.clicks,
  spend.spend,
  daily_hubspot_performances.deal_amount,
  daily_hubspot_performances.proposals,
  daily_hubspot_performances.closings
FROM
  dimensions
  LEFT JOIN clients
  ON dimensions.client = clients.clientName
  AND dimensions.source = clients.platform
  LEFT JOIN spend
  ON dimensions.date = spend.date
  AND clients.accountName = spend.client
  AND dimensions.source = spend.source
  LEFT JOIN daily_hubspot_performances
  ON dimensions.date = daily_hubspot_performances.date
  AND clients.clientName = daily_hubspot_performances.client
  AND dimensions.source = daily_hubspot_performances.source
ORDER BY
  dimensions.date,
  dimensions.month,
  dimensions.client,
  dimensions.source
