{{ config(schema='STAGING', materialized='table') }}

with base as (
    select
        nullif(trim(provider), '') as provider,
        case upper(nullif(trim(potentialfraud), ''))
            when 'YES' then 1
            when 'Y'   then 1
            when '1'   then 1
            when 'NO'  then 0
            when 'N'   then 0
            when '0'   then 0
            else null
        end as potentialfraud
    from {{ source('source_raw_data','provider_raw') }}
),
filtered as (
    select *
    from base
    where provider is not null
)
select *
from filtered
qualify row_number() over (
    partition by provider
    order by (potentialfraud is not null) desc
) = 1
