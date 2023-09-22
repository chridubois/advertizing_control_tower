{{ config(
  materialized = 'view'
) }}

{% set accounts =  ['google_ads_izidore_google_ads', 'google_ads_begoodz_google_ads'] %}

{% for account in accounts %}
  select
      *,
      CASE
      WHEN '{{ account }}' = 'google_ads_izidore_google_ads' THEN 'Izidore'
      WHEN '{{ account }}' = 'google_ads_begoodz_google_ads' THEN 'Begoodz'
      END as client,
  from {{ source(account, 'google_ads__campaign_report') }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
