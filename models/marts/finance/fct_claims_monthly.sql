{{ config(schema='MARTS_FINANCE', materialized='table') }}

select
    date_trunc(month, claim_start_date) as month_start_date,
    claim_type,
    count(distinct claimid) as total_claims,
    sum(reimbursed_amount) as total_reimbursed,
    sum(deductible_paid) as total_deductible
from {{ ref('int_claims_enriched') }}
group by 1, 2
