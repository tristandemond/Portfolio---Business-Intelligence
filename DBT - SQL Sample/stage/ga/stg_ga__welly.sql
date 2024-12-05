{{ config(
      tags=['welly', 'stage', 'ga']
) }}

with
  unioned as (
    {{ dbt_utils.union_relations(
        relations=[
          ref('base_ga__session__welly')
          , ref('base_ga__ecommerce_performance__welly')
          ]
        ,exclude=['_dbt_source_relation','_dw_inserted_at']
    ) }}
  )


  ,transformed_level1 as (
      select
        date
        ,connection
        ,device
        ,campaign
        ,productname as product
        ,adgroup as ad_group
        ,campaign as utm_campaign
        ,medium as utm_medium
        ,source as utm_source
        ,adcontent as utm_content
        ,case
           when (lower(utm_source) = 'google' and utm_medium = 'cpc') then 'Google'
           when (lower(utm_source) = 'youtube' and utm_campaign like 'tin_welly%') then 'Google'
           when (lower(utm_source) = 'bing' and lower(utm_medium) = 'cpc') then 'Bing'
           when (lower(utm_source) = 'snapchat' and lower(utm_medium) = 'cpc') then 'Snapchat'
           when (lower(utm_source) = 'facebook' and lower(utm_medium) = 'paidsocial') then 'Facebook'
           when (lower(utm_source) = 'pinterest' and lower(utm_medium) = 'paidsocial') then 'Pinterest'
           when lower(utm_source) = 'dotdash' then 'Dotdash'
           when lower(utm_source) = 'ttd' then 'TTD'
           when lower(utm_source) = 'healthline' then 'Healthline'
           when lower(utm_source) = 'triplelift' then 'Triplelift'
           when lower(utm_source) = 'spotify' then 'Spotify'
           when lower(utm_source) = 'innovid' then 'Innovid'
           when lower(utm_source) = 'wunderkind' then 'TTD'
           when (utm_campaign like 'tin_welly%' and lower(utm_campaign) = 'tiktok') then 'TikTok'
           when utm_campaign like 'tin_welly%' then upper(left(utm_source,1)) + lower(right(utm_source,len(utm_source)-1))
           else 'Unknown'
         end as engine
        ,usertype
        ,_dw_connection
        ,insert_date
        
        ,sessions
        ,bounces
        ,pageviews
        ,conversions_ga
        ,revenue_ga
        ,uniquepurchases
        ,itemrevenue as product_revenue
        ,itemquantity as item_quantity

      from unioned 
  )

  ,aggregated_transformed_level2 as (
      select
        date
        , connection
        , case
            when (engine = 'Google' or engine = 'Bing') then engine || ' ' ||
              case
              when lower(utm_campaign) like '%smart shopping%' then 'Shopping Smart'
              when lower(utm_campaign) like '%\\_shop\\_brand%' then 'Shopping Brand'
              when lower(utm_campaign) like '%\\_shop\\_%' then 'Shopping Nonbrand'
              when lower(utm_campaign) like '%youtube%' then 'YouTube Brand'
              when (lower(utm_source) like '%youtube%' and lower(utm_campaign) != '(not set)') then 'YouTube Brand'
              when (utm_campaign = 'Brand Terms' or lower(utm_campaign) like '%\\_brand%') then 'Search Brand'
              else 'Search Nonbrand'
              end
            when engine = 'Facebook' then engine || ' ' ||
              case
              when utm_campaign ~ 'Prospecting|DABA|lal|LAL' then 'Prospecting'
              when lower(utm_campaign) like '%retention%' then 'Retention'
              when lower(utm_campaign) ~ 'retargeting|dpa' then 'Retargeting'
              else 'Other'
              end
            when lower(utm_source) ~ 'dotdash|healthline|innovid|ttd|wunderkind|triplelift|spotify' then 'Brand Awareness'
            else engine
          end as tactic 
        , case
            when device = 'desktop' then 'Desktop'
            when device = 'mobile' then 'Mobile'
            when device = 'tablet' then 'Tablet'
            else 'Unknown'
          end as device
        , campaign 
        , product
        , case
            when lower(product) like '%bandages%' then 'Bandages'
            when lower(product) like '%kit%' then 'Kits'
            when lower(product) like '%kits%' then 'Kits'
            when lower(product) like '%bundle%' then 'Bundles'
            when lower(product) like '%hydrocolloid%' then 'Hydrocolloids'
            when lower(product) like '%refill%' then 'Refills'
            else 'Unknown'
          end as product_category
        , ad_group
        , utm_campaign
        , utm_content
        , utm_medium
        , utm_source
        , engine
        , _dw_connection
        , max(insert_date) as insert_date
        
        , sum(case
                when usertype = 'New Visitor' then conversions_ga
              end) as conversions_ntb
        , sum(case
                when usertype = 'New Visitor' then revenue_ga
              end) as revenue_ntb
        , sum(case
                when usertype = 'New Visitor' then sessions
              end) as new_user_sessions
        , sum(sessions) as sessions
        , sum(bounces) as bounces
        , sum(pageviews) as pageviews
        , sum(conversions_ga) conversions_ga
        , sum(revenue_ga) as revenue_ga
        , sum(uniquepurchases) as unique_purchases
        , sum(product_revenue) product_revenue
        , sum(item_quantity) as item_quantity

      from transformed_level1
      {{ dbt_utils.group_by(14) }}
  )

  ,transformed_level3 as (
    select
      date
      , connection
      , case
          when engine = 'Copy_link' then 'TikTok'
          when engine = 'Tiktok' then 'TikTok'
          else engine
        end as engine
      , case
          when tactic = 'Copy_link' then 'TikTok'
          when tactic = 'Tiktok' then 'TikTok'
          else tactic
        end as tactic
      , case
          when lower(tactic) like '%nonbrand%' then 'Nonbrand'
          when lower(tactic) like '%brand%' then 'Brand'
          else 'Unknown'
        end as campaign_branded
      , case
          when lower(tactic) like '%search%' then 'Search'
          when lower(tactic) like '%shopping%' then 'Shopping'
          when lower(utm_source) ~ 'facebook|pinterest|snapchat|fb|tiktok' then 'Social'
          when lower(utm_medium) ~ 'facebook|pinterest|snapchat|fb|tiktok' then 'Social'
          when lower(utm_source) like '%email%' then 'email'
          when lower(utm_medium) like '%email%' then 'email'
          when ((lower(utm_campaign) like '%youtube%' or lower(utm_source) like '%youtube%') and lower(utm_campaign) != '(not set)') then 'Programmatic'
          when lower(utm_source) ~ 'dotdash|healthline|innovid|ttd|wunderkind|triplelift|spotify' then 'Programmatic'
          else 'Unknown'
        end as channel
      , device
      , campaign
      , product_category  
      , product
      , ad_group
      , utm_campaign
      , utm_medium
      , utm_source
      , utm_content
      , _dw_connection
      , insert_date
      
      , conversions_ntb
      , revenue_ntb
      , new_user_sessions
      , sessions
      , bounces
      , pageviews
      , conversions_ga
      , revenue_ga
      , unique_purchases
      , product_revenue
      , item_quantity

    from aggregated_transformed_level2
    where engine != 'Unknown'
  )

select *
from transformed_level3