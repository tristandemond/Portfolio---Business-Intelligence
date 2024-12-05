{{ config(
      tags=['client', 'stage']
) }}

with
  source as (
    select *
    from {{ ref('int_unioned_client') }}
  )

, filtered as (
    select
    date
    ,device
    ,tactic
    ,product_category
    ,channel
    ,campaign_branded
    ,connection
    ,engine
    ,tactic_pacing
    ,utm_content
    ,campaign
    ,product
    ,ad_group
    ,utm_campaign
    ,utm_medium
    ,utm_source
    ,ad_creative
    ,state
    ,ad
    ,insert_date

    ,conversions_platform
    ,impressions
    ,clicks
    ,link_clicks
    ,engagement
    ,comments
    ,sessions
    ,bounces
    ,revenue_platform
    ,conversions_ntb
    ,revenue_ntb
    ,rev_ntb
    ,conv_ntb
    ,conversions_ga
    ,revenue_ga
    ,new_user_sessions
    ,unique_purchases
    ,product_revenue
    ,item_quantity
    ,pageviews
    ,video_views
    ,eligible_impressions
    ,impressions_for_calc_use_only
    ,spend1000
    ,spend_ga
    ,spend_conv
    ,clicks_ga
    ,clicks_conv
    ,spend_rev
    ,spend

    from source

    )

select *

from filtered