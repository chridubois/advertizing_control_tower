{{ config(
  materialized = 'view'
) }}

WITH google_ads_campaign AS (

  SELECT
    *
  FROM
    {{ ref('stg_google_ads_campaigns') }}
),
google_ads_campaign_stats AS (
  SELECT
    *
  FROM
    {{ ref('stg_google_ads_campaigns_stats') }}
)
SELECT
  google_ads_campaign_stats.*,
  google_ads_campaign.campaign_name AS campaign_name,
  google_ads_campaign.status AS campaign_status,
FROM
  google_ads_campaign_stats
  LEFT JOIN google_ads_campaign
  ON (
    google_ads_campaign_stats.id = google_ads_campaign.campaign_id
    AND google_ads_campaign_stats.client = google_ads_campaign.client
    AND google_ads_campaign_stats.date = google_ads_campaign.date_day
  )
