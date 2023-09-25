{{ config(
  materialized = 'view'
) }}

WITH dimensions AS (

  SELECT
    *
  FROM
    {{ ref('monthly_dimensions') }}
),
spend AS (
  SELECT
    *
  FROM
    {{ ref('monthly_spend') }}
),
monthly_hubspot_performances AS (
  SELECT
    *
  FROM
    {{ ref('monthly_hubspot_performances') }}
),
clients AS (

  SELECT *
  FROM
    {{ ref('clients') }}
)
SELECT DISTINCT
  dimensions.month,
  dimensions.client,
  dimensions.source,
  spend.impressions,
  spend.clicks,
  spend.spend,
  monthly_hubspot_performances.deal_amount,
  monthly_hubspot_performances.proposals,
  monthly_hubspot_performances.closings,
  -- spend.spend / proposals.proposals AS cpl,
  -- closings.closings / proposals.proposals AS closingRate,
  -- spend.spend / closings.closings AS cpa,
  -- spend.spend / closings.closings * 100 AS roas
FROM
  dimensions
  LEFT JOIN clients
  ON dimensions.client = clients.clientName
  AND dimensions.source = clients.platform
  LEFT JOIN spend
  ON dimensions.month = spend.month
  AND clients.accountName = spend.client
  AND dimensions.source = spend.source
  LEFT JOIN monthly_hubspot_performances
  ON dimensions.month = monthly_hubspot_performances.month
  AND clients.accountName = monthly_hubspot_performances.client
  AND dimensions.source = monthly_hubspot_performances.source
ORDER BY
  dimensions.month,
  dimensions.client,
  dimensions.source
