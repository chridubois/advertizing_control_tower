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
  date,
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
  CAST(
    campaign_id AS INTEGER
  ) AS campaign_id,
  action_type,
  value
FROM
  basic_campaign_actions
