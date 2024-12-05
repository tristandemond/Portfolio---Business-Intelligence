{{ config(
      tags=['client', 'stage', 'social', 'tiktok']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('base_tiktok__ads__client')
          , ref('base_tiktok__ads_conversion__client')
          , ref('base_tiktok__ads_geo__client')
          ]
    ) }}
  )

  ,aggregated as (
    select
      report_date as date
      ,'Mobile'::varchar as device
      ,case
         when split_part(lower(campaign_name),'_',4)::varchar = 'brand' then 'Brand'
         when split_part(lower(campaign_name),'_',4)::varchar = 'nonbrand' then 'Nonbrand'
       end as campaign_branded
      ,'TikTok'::varchar as engine
      ,split_part(lower(campaign_name),'_',6) as tactic
      ,'Social'::varchar as channel
      ,region as state
      ,_dw_connection
      ,'TikTok'::varchar as connection
      ,campaign_name as campaign
      ,ad_name as ad
      ,ad_group_name as ad_group
      ,ad_creative
      ,max(insert_date) as insert_date

      ,sum(cost) as spend
      ,sum(impression) as impressions
      ,sum(click) as clicks
      ,sum(video_views) as video_views
      ,sum(case 
            when conversion_name = 'total complete payment' 
            then conversions  
           end)::integer as "conversions_platform"
      ,sum(case 
            when conversion_name = 'total complete payment value' 
            then conversion_value 
           end)::numeric(14,7) as "revenue_platform"

    from unioned  
    {{ dbt_utils.group_by(13) }}
  )


select *

from aggregated