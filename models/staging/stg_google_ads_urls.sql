{{ config(
  materialized = 'view'
) }}

{% set accounts =  ['google_ads_izidore_google_ads', 'google_ads_begoodz_google_ads'] %}

{% for account in accounts %}
  select
      *,
      '{{ account }}' as account
  from {{ source(account, 'google_ads__url_report') }}
{% if not loop.last -%} union all {%- endif %}
{% endfor %}
