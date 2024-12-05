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
      ,'Spotify'::varchar as connection
      ,_dw_connection
      ,'Programmatic':: varchar as channel
      ,'Brand Awareness':: varchar as tactic
      ,'Brand'::varchar as campaign_branded
      ,ad_group
      ,ad_creative
      ,campaign
      ,'Spotify'::varchar as engine
      ,max(insert_date) as insert_date

      ,sum(impressions) as impressions
      ,sum(spend) as spend
      ,sum(clicks) as clicks 

    from source 
    {{ dbt_utils.group_by(10) }}
  )

select *

from aggregated