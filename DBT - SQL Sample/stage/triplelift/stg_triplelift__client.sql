{{ config(
      tags=['client', 'stage', 'triplelift']
) }}

with
  source as (
    select *
    from {{ ref('base_triplelift__reporting__client') }}
  )

  ,aggregated as (
    select
      report_date as date
      ,'Triplelift'::varchar as engine
      ,'Programmatic'::varchar as channel
      ,'Brand Awareness'::varchar as tactic
      ,'Brand'::varchar as campaign_branded
      ,'Triplelift'::varchar as connection
      ,_dw_connection
      ,publisher as ad_group
      ,max(insert_date) as insert_date

      ,sum(views * 0.45)::numeric(14,7) as spend

    from source
    {{ dbt_utils.group_by(8) }}
  )

select *

from aggregated