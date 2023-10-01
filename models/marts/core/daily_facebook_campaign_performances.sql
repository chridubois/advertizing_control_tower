{{ config(
  materialized = 'view'
) }}

WITH facebook_daily_actions AS (

  SELECT
    *
  FROM
    {{ ref('stg_facebook_ads_daily_campaigns_actions') }}
),
facebook_campaign_stats AS (
  SELECT
    date_day,
    account_id,
    campaign_id,
    campaign_name,
    status,
    clicks,
    impressions,
    spend
  FROM
    {{ ref('stg_facebook_ads_campaigns') }}
),
clients AS (
  SELECT
    *
  FROM
    {{ ref('clients') }}
)
SELECT
  facebook_campaign_stats.*,
  clients.clientName AS client,
  LEAD.value AS leads,
  purchase.value AS purchases,
  add_to_cart.value AS add_to_carts,
  video_view.value AS video_views,
  page_engagement.value AS page_engagements
FROM
  facebook_campaign_stats
  LEFT JOIN clients ON clients.accountId = facebook_campaign_stats.account_id
  AND clients.platform = 'facebook'
  LEFT JOIN facebook_daily_actions LEAD
  ON LEAD.date = facebook_campaign_stats.date_day
  AND LEAD.campaign_id = facebook_campaign_stats.campaign_id
  AND LEAD.action_type = 'lead'
  LEFT JOIN facebook_daily_actions purchase
  ON purchase.date = facebook_campaign_stats.date_day
  AND purchase.campaign_id = facebook_campaign_stats.campaign_id
  AND purchase.action_type = 'purchase'
  LEFT JOIN facebook_daily_actions add_to_cart
  ON add_to_cart.date = facebook_campaign_stats.date_day
  AND add_to_cart.campaign_id = facebook_campaign_stats.campaign_id
  AND add_to_cart.action_type = 'add_to_cart'
  LEFT JOIN facebook_daily_actions video_view
  ON video_view.date = facebook_campaign_stats.date_day
  AND video_view.campaign_id = facebook_campaign_stats.campaign_id
  AND video_view.action_type = 'video_view'
  LEFT JOIN facebook_daily_actions page_engagement
  ON page_engagement.date = facebook_campaign_stats.date_day
  AND page_engagement.campaign_id = facebook_campaign_stats.campaign_id
  AND page_engagement.action_type = 'page_engagement'
