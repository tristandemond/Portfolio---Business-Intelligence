{{ config(
      tags=['client', 'stage', 'sizmek']
) }}

with
  source as (
    select *
    from {{ ref('base_sizmek__daily__client') }}
  )

  ,aggregated as (
    select
      report_date as date
      ,'Sizmek'::varchar as connection
      ,'Programmatic'::varchar as channel
      ,'Brand Awareness'::varchar as tactic
      ,'Brand'::varchar as campaign_branded
      ,_dw_connection
      ,campaign_name::varchar as campaign
      ,case
         when lower(site_name) like '%spotify%' then 'Spotify'
         when lower(site_name) like '%google%' then 'Google'
         when lower(site_name) like '%dotdash%' then 'Dotdash'
         when lower(site_name) like '%the trade desk%' then 'TTD'
         when lower(site_name) like '%amazon%' then 'Amazon'
         when lower(site_name) like '%healthline%' then 'Healthline'
         when lower(site_name) like '%triplelift%' then 'Triplelift'
         when lower(site_name) like '%wunderkind%' then 'TTD'
         else site_name
       end as engine
      ,ad
      ,ad_creative
      ,max(insert_date) as insert_date

      ,sum(impressions_net) as impressions
      ,sum(clicks_net) as clicks

      from source 
      {{ dbt_utils.group_by(10) }}
  )

  , transformed_level1 as (
    select 
      date
      ,connection
      ,channel
      ,tactic
      ,campaign_branded
      ,campaign
      ,engine
      ,_dw_connection
      ,ad
      ,ad_creative
      ,insert_date

      ,impressions
      ,clicks
      ,case
         when lower(ad_creative) like '%otccampaign%' then
           case
             when engine = 'Healthline' and lower(ad_creative) like '%addedvalue%' then null
             when engine = 'Healthline' and lower(ad_creative) like '%highimpact%' then impressions*0.015
             when engine = 'Healthline' and lower(ad_creative) like '%banner%' then impressions*0.011
           end
         else null
       end as spend
      
      from aggregated
  )

select *

from transformed_level1
where engine != 'Spotify'

