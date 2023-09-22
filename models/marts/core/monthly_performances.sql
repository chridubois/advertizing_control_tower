{{ config(
  materialized = 'view'
) }}

WITH dimensions AS (

  SELECT
    *
  FROM
    {{ ref('monthly_dimensions') }}
),
proposals AS (
  SELECT
    *
  FROM
    {{ ref('monthly_proposals') }}
),
closings AS (
  SELECT
    *
  FROM
    {{ ref('monthly_closings') }}
),
spend AS (
  SELECT
    *
  FROM
    {{ ref('monthly_spend') }}
)
SELECT
  dimensions.month,
  dimensions.client,
  dimensions.source,
  proposals.proposals,
  closings.closings,
  spend.spend
FROM
  dimensions
  LEFT JOIN proposals
  ON dimensions.month = proposals.month
  AND dimensions.client = proposals.client
  AND dimensions.source = proposals.source
  LEFT JOIN closings
  ON dimensions.month = closings.month
  AND dimensions.client = closings.client
  AND dimensions.source = closings.source
  LEFT JOIN spend
  ON dimensions.month = spend.month
  AND dimensions.client = spend.client
  AND dimensions.source = spend.source
ORDER BY
  dimensions.month,
  dimensions.client,
  dimensions.source
