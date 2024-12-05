{{ config(
    tags=['client', 'attentive', 'base', 'sms']
) }}

with 
  source as (
    select * 
    from {{ source('client', 'attentive__sms') }}
    where client = 'client'
  )

  ,filtered as (
    select
      day::date
      ,message_name::varchar
      ,'v_upload_attentive_sms'::varchar as _dw_connection
      ,insert_date::date as insert_date

      ,delivered::integer
      ,clicks::integer
      ,conversions::integer
      ,revenue::numeric(14,7)
      
    from source
  )
  
select *
from filtered