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
)
SELECT
  dimensions.month,
  dimensions.client,
  dimensions.source,
  proposals.proposals,
  closings.closings
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
