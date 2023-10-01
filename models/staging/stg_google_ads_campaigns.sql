{{ config(
  materialized = 'view'
) }}

{% set accounts = ['google_ads_izidore_google_ads', 'google_ads_begoodz_google_ads', 'google_ads_ensol_google_ads'] %}
{% for account in accounts %}

  SELECT
    *,
    CASE
      WHEN '{{ account }}' = 'google_ads_izidore_google_ads' THEN 'Izidore'
      WHEN '{{ account }}' = 'google_ads_begoodz_google_ads' THEN 'Begoodz'
      WHEN '{{ account }}' = 'google_ads_ensol_google_ads' THEN 'ensol'
    END AS client
  FROM
    {{ source(
      account,
      'google_ads__campaign_report'
    ) }}

    {% if not loop.last -%}
    UNION ALL
    {%- endif %}
  {% endfor %}
