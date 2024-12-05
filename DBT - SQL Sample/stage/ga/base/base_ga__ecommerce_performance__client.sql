{{ config(
    tags=['client', 'base', 'ga']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'ga__ecommerce') }}
    where client = 'client'
  )

  ,filtered as (
    select
      report_date::date as date  
      ,campaign::varchar
      ,productname::varchar
      ,devicecategory::varchar as device
      ,medium::varchar
      ,source::varchar
      ,'Google Analytics Product'::varchar as connection 
      ,'v_ga_ecommerce_enhanced'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,uniquepurchases::integer
      ,itemrevenue::numeric(14,7)
      ,itemquantity::numeric(14,7)
      
    from source
    where report_date < current_date
  )
  
select *
from filtered