{{ config(schema='MARTS_FRAUD_ANALYSIS', materialized='table') }}

select
    p.beneid,
    p.total_claims,
    p.unique_providers,
    p.total_reimbursed,
    p.total_deductible,
    p.avg_claim_amount,
    p.post_death_claims,
    p.high_value_claims,
    g.gender as gender_label,
    p.current_age,
    p.chronic_condition_count,
    p.is_deceased
from {{ ref('int_patient_aggregates') }} p
left join {{ ref('gender_lookup') }} g
  on p.gender = g.gender_id
