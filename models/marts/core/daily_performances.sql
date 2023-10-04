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
),
budget_ensol AS (
  SELECT
    *
  FROM
    {{ source(
      'google_sheet_budget_ensol',
      'google_sheet_budget_ensol'
    ) }}
)
SELECT
  dimensions.date,
  dimensions.month,
  dimensions.client,
  dimensions.source,
  spend.impressions,
  spend.clicks,
  spend.spend,
  (
    IF(
      EXTRACT(
        DAY
        FROM
          dimensions.date
      ) = 1,
      budget_ensol.budget,
      0
    )
  ) AS budget,
  daily_hubspot_performances.deal_amount,
  daily_hubspot_performances.contacts,
  daily_hubspot_performances.deals,
  daily_hubspot_performances.quote_sent,
  daily_hubspot_performances.quote_signed,
  daily_hubspot_performances.quote_pending_high_probability,
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
  LEFT JOIN budget_ensol
  ON dimensions.client = 'ensol'
  AND dimensions.source = budget_ensol.platform
ORDER BY
  dimensions.date,
  dimensions.month,
  dimensions.client,
  dimensions.source
