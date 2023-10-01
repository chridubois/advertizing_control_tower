{{ config(
  materialized = 'view'
) }}

WITH taboola_campaigns_ensol AS (

  SELECT
    *,
    'taboola' AS source,
    'ensol' AS client,
  FROM
    {{ source(
      'taboola_ensol',
      'campaign_site_day_report'
    ) }}
)
SELECT *
FROM taboola_campaigns_ensol
