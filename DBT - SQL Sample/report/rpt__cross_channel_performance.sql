{{ config(
      tags=['welly', 'report']
) }}

with
  source as (
    select *
    from {{ ref('stg_union__welly') }}
  )

select *
from source