{{ config(
  materialized = 'view'
) }}

WITH fb_monthly_campaign_actions AS (

  SELECT
    MONTH,
    account_id,
    SUM(link_clicks) AS link_clicks,
    SUM(leads) AS leads,
    SUM(purchases) AS purchases,
    SUM(add_to_carts) AS add_to_carts,
    SUM(landing_page_views) AS landing_page_views,
    SUM(video_views) AS video_views,
    SUM(page_engagements) AS page_engagements
  FROM
    {{ ref('monthly_facebook_actions') }}
  GROUP BY
    MONTH,
    account_id
),
facebook_ads_spend AS (
  SELECT
    CONCAT(
      EXTRACT(
        YEAR
        FROM
          date_day
      ),
      '-',
      EXTRACT(
        MONTH
        FROM
          date_day
      )
    ) AS MONTH,
    account_id,
    account_name,
    'facebook' AS source,
    SUM(
      spend
    ) AS spend,
    SUM(
      clicks
    ) AS clicks,
    SUM(
      impressions
    ) AS impressions
  FROM
    {{ ref('stg_facebook_ads_campaigns') }}
  GROUP BY
    MONTH,
    account_id,
    account_name,
    source
)
SELECT
  fb_monthly_campaign_actions.month,
  facebook_ads_spend.account_name AS client,
  facebook_ads_spend.source,
  fb_monthly_campaign_actions.link_clicks,
  fb_monthly_campaign_actions.leads,
  fb_monthly_campaign_actions.purchases,
  fb_monthly_campaign_actions.add_to_carts,
  fb_monthly_campaign_actions.landing_page_views,
  fb_monthly_campaign_actions.video_views,
  fb_monthly_campaign_actions.page_engagements,
  facebook_ads_spend.spend,
  facebook_ads_spend.clicks,
  facebook_ads_spend.impressions
FROM
  fb_monthly_campaign_actions
  LEFT JOIN facebook_ads_spend
  ON fb_monthly_campaign_actions.month = facebook_ads_spend.month
  AND fb_monthly_campaign_actions.account_id = facebook_ads_spend.account_id
