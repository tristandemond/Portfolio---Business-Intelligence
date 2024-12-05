{{ config(
    tags=['client','klaviyo', 'stage', 'email']
) }}

with 
  source as (
    select * 
    from {{ ref('base_klaviyo__email__client') }}
  )

  ,aggregated as (
    select
      date
      ,campaign_name as campaign
      ,_dw_connection
      ,'Klaviyo'::varchar as engine
      ,'Email'::varchar as channel
      ,'Brand'::varchar as campaign_branded
      ,'Klaviyo'::varchar as connection

      ,sum(deliveries) as impressions
      ,sum(clicks) as clicks
      ,sum(conversions) as conversions_platform
      ,sum(bounces) as bounces

    from source
    {{ dbt_utils.group_by(7) }}
  )
  
select *
from aggregated

