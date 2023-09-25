{{ config(
  materialized = 'view'
) }}

WITH fb_monthly_campaign_actions AS (

  SELECT
    *
  FROM
    {{ ref('stg_facebook_ads_monthly_campaigns_actions') }}
),
campaign_history AS (
  SELECT
    *
  FROM
    {{ source(
      'facebook_ads2',
      'campaign_history'
    ) }}
),
link_clicks AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS link_clicks
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'link_click'
  GROUP BY
    MONTH,
    campaign_id
),
leads AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS leads
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'lead'
  GROUP BY
    MONTH,
    campaign_id
),
purchases AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS purchases
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'purchase'
  GROUP BY
    MONTH,
    campaign_id
),
add_to_carts AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS add_to_carts
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'add_to_cart'
  GROUP BY
    MONTH,
    campaign_id
),
landing_page_views AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS landing_page_views
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'landing_page_view'
  GROUP BY
    MONTH,
    campaign_id
),
video_views AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS video_views
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'video_view'
  GROUP BY
    MONTH,
    campaign_id
),
page_engagements AS (
  SELECT
    MONTH,
    campaign_id,
    SUM(action_value) AS page_engagements
  FROM
    fb_monthly_campaign_actions
  WHERE
    action_type = 'page_engagement'
  GROUP BY
    MONTH,
    campaign_id
)
SELECT
  fb_monthly_campaign_actions.month,
  campaign_history.account_id,
  fb_monthly_campaign_actions.campaign_id,
  link_clicks.link_clicks,
  leads.leads,
  purchases.purchases,
  add_to_carts.add_to_carts,
  landing_page_views.landing_page_views,
  video_views.video_views,
  page_engagements.page_engagements
FROM
  fb_monthly_campaign_actions
  LEFT JOIN campaign_history
  ON fb_monthly_campaign_actions.campaign_id = campaign_history.id
  LEFT JOIN link_clicks
  ON fb_monthly_campaign_actions.campaign_id = link_clicks.campaign_id
  AND fb_monthly_campaign_actions.month = link_clicks.month
  LEFT JOIN leads
  ON fb_monthly_campaign_actions.campaign_id = leads.campaign_id
  AND fb_monthly_campaign_actions.month = leads.month
  LEFT JOIN purchases
  ON fb_monthly_campaign_actions.campaign_id = purchases.campaign_id
  AND fb_monthly_campaign_actions.month = purchases.month
  LEFT JOIN add_to_carts
  ON fb_monthly_campaign_actions.campaign_id = add_to_carts.campaign_id
  AND fb_monthly_campaign_actions.month = add_to_carts.month
  LEFT JOIN landing_page_views
  ON fb_monthly_campaign_actions.campaign_id = landing_page_views.campaign_id
  AND fb_monthly_campaign_actions.month = landing_page_views.month
  LEFT JOIN video_views
  ON fb_monthly_campaign_actions.campaign_id = video_views.campaign_id
  AND fb_monthly_campaign_actions.month = video_views.month
  LEFT JOIN page_engagements
  ON fb_monthly_campaign_actions.campaign_id = page_engagements.campaign_id
  AND fb_monthly_campaign_actions.month = page_engagements.month
