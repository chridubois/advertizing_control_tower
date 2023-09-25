{{ config(
  materialized = 'table'
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
  CAST(
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
      ),
      '-',
      EXTRACT(
        DAY
        FROM
          DATE
      )
    ) AS DATE
  ) AS DATE,
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
  SUM(VALUE) AS action_value
FROM
  basic_campaign_actions
GROUP BY
  date,
  MONTH,
  campaign_id,
  action_type
