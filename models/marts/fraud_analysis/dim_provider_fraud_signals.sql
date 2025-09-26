{{ config(schema='MARTS_FRAUD_ANALYSIS', materialized='table') }}

select
    p.provider,
    p.claim_type,
    p.total_claims,
    p.unique_patients,
    p.total_reimbursed,
    p.total_deductible,
    p.avg_claim_amount,
    p.avg_length_of_stay,
    p.post_death_claims,
    p.high_value_claims,
    l.flag as potentialfraud

from {{ ref('int_provider_aggregates') }} p
left join {{ ref('yes_no_lookup') }} l
    on p.potentialfraud = l.flag_id
