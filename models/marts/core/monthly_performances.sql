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
daily_hubspot_performances AS (
  SELECT
    MONTH,
    client,
    source,
    SUM(contacts) AS contacts,
    SUM(deal_amount) AS deal_amount,
    SUM(deals) AS deals,
    SUM(quote_sent) AS quote_sent,
    SUM(quote_signed) AS quote_signed
  FROM
    {{ ref('daily_hubspot_performances') }}
  GROUP BY
    MONTH,
    client,
    source
),
clients AS (
  SELECT
    *
  FROM
    {{ ref('clients') }}
)
SELECT
  dimensions.month_of_the_year,
  dimensions.client,
  dimensions.source,
  spend.impressions,
  spend.clicks,
  spend.spend,
  daily_hubspot_performances.deal_amount,
  daily_hubspot_performances.contacts,
  daily_hubspot_performances.deals,
  daily_hubspot_performances.quote_sent,
  daily_hubspot_performances.quote_signed,
FROM
  dimensions
  LEFT JOIN clients
  ON dimensions.client = clients.clientName
  AND dimensions.source = clients.platform
  LEFT JOIN spend
  ON dimensions.month_of_the_year = spend.month
  AND clients.accountName = spend.client
  AND dimensions.source = spend.source
  LEFT JOIN daily_hubspot_performances
  ON dimensions.month_of_the_year = daily_hubspot_performances.month
  AND clients.clientName = daily_hubspot_performances.client
  AND dimensions.source = daily_hubspot_performances.source
ORDER BY
  dimensions.month_of_the_year,
  dimensions.client,
  dimensions.source
