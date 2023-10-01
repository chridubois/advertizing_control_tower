{{ config(
  materialized = 'view'
) }}

{% set accounts =  ['google_ads_izidore', 'google_ads_begoodz', 'google_ads_ensol'] %}

{% for account in accounts %}
  select
      *,
      CASE
      WHEN '{{ account }}' = 'google_ads_izidore' THEN 'Izidore'
      WHEN '{{ account }}' = 'google_ads_begoodz' THEN 'Begoodz'
      WHEN '{{ account }}' = 'google_ads_ensol' THEN 'ensol'
      END as client
  from {{ source(account, 'campaign_stats') }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
