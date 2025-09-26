{{ config(schema='MARTS_FINANCE', materialized='table') }}

with claim_types as (
    select distinct claim_type from {{ ref('int_claims_enriched') }}
),

months as (
    select distinct
        year(date_day)                as year,
        month(date_day)               as month,
        to_char(date_day,'Mon')       as month_name,
        quarter(date_day)             as quarter
    from {{ ref('dim_time') }}
),

monthly_claims as (
    select
        extract(year from claim_start_date)  as claim_year,
        extract(month from claim_start_date) as claim_month,
        claim_type,
        count(distinct claimid)              as total_claims,
        sum(reimbursed_amount)               as total_reimbursed,
        sum(deductible_paid)                 as total_deductible
    from {{ ref('int_claims_enriched') }}
    group by 1,2,3
)

select
    m.year,
    m.month,
    m.month_name,
    m.quarter,
    ct.claim_type,
    coalesce(c.total_claims, 0)      as total_claims,
    coalesce(c.total_reimbursed, 0)  as total_reimbursed,
    coalesce(c.total_deductible, 0)  as total_deductible
from months m
cross join claim_types ct
left join monthly_claims c
    on m.year = c.claim_year
   and m.month = c.claim_month
   and ct.claim_type = c.claim_type
order by m.year, m.month, ct.claim_type
