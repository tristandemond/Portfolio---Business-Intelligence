{{ config(
    tags=['client', 'base', 'triplelift']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'triplelift__reporting') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date
      ,'v_upload_triplelift_reporting'::varchar as _dw_connection
      ,publisher::varchar
      ,insert_date::date as insert_date

      ,views::integer

    from source
  )
  
select *
from filtered
