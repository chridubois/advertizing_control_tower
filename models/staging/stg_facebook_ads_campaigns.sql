{{ config(
  materialized = 'view'
) }}

WITH facebook_ads_campaing_report AS (

  SELECT
    *
  FROM
    {{ source(
      'facebook_ads2_facebook_ads',
      'facebook_ads__campaign_report'
    ) }}
)
SELECT *
FROM facebook_ads_campaing_report
