{{ config(
  materialized = 'view'
) }}

WITH basic_campaign_actions AS (

  SELECT
    *
  FROM
    {{ source(
      'facebook_ads2',
      'basic_campaign_actions'
    ) }}
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
  CAST(campaign_id AS INTEGER) AS campaign_id,
  action_type,
  SUM(VALUE) AS action_value
FROM
  basic_campaign_actions
GROUP BY
  MONTH,
  campaign_id,
  action_type
