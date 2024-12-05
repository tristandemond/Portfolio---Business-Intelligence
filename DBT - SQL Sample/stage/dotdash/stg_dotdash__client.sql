{{ config(
      tags=['welly', 'stage', 'programmatic']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('base_dotdash__brand__welly')
          , ref('base_dotdash__campaign__welly')
          ]
    ) }}
  )

  ,aggregated_transformed_level1 as (
      select
        report_date as date
        ,'Brand'::varchar as campaign_branded
        ,'DotDash'::varchar as tactic
        ,'Programmatic'::varchar as channel
        ,'Dotdash'::varchar as engine
        ,'DotDash'::varchar as connection
        ,_dw_connection
        ,ad
        ,campaign
        ,max(insert_date) as insert_date

        ,sum(impressions) as impressions
        ,sum(clicks) as clicks
        ,sum(coalesce(revenue_platform,0)) as revenue_platform
        ,sum(pageviews) as pageviews
        ,sum(coalesce(spend_custom_brand_review,0)) as spend_custom_brand_review
        ,sum(coalesce(spend_personalized_prepared_quiz,0)) as spend_personalized_prepared_quiz
        ,sum(coalesce(spend_road_trip_essentials,0)) as spend_road_trip_essentials

      from unioned  
      {{ dbt_utils.group_by(9) }}

  )

,transformed_level2 as (
      select
        date
        , campaign_branded
        , tactic
        , channel
        , engine
        , connection
        , _dw_connection
        , ad
        , campaign
        , insert_date

        , impressions
        , clicks
        , revenue_platform
        , pageviews
        , spend_custom_brand_review
        , spend_personalized_prepared_quiz
        , spend_road_trip_essentials
        , spend_custom_brand_review+spend_personalized_prepared_quiz+spend_road_trip_essentials+revenue_platform as spend        

      from aggregated_transformed_level1  

  )

select *

from transformed_level2