{{ config(
      tags=['client', 'stage', 'sprout']
) }}

with
  source as (
    select *
    from {{ ref('base_sprout__daily__client') }}
  )

  ,aggregated as (
      select
        report_date as date
        ,'Sprout'::varchar as connection
        ,_dw_connection
        ,'Social'::varchar as channel
        ,'Sprout'::varchar as engine
        ,max(insert_date) as insert_date

        ,sum(post_link_clicks+other_post_clicks) as clicks
        ,sum(impressions) as impressions
        ,sum(video_views) as video_views
        ,sum(engagements) as engagement
        ,sum(post_link_clicks) as link_clicks
        ,sum(comments) as comments
  
      from source
      {{ dbt_utils.group_by(5) }}

  )

select *

from aggregated