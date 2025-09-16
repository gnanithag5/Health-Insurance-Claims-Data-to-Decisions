{{ config(schema='MARTS_FRAUD_ANALYSIS', materialized='table') }}

select
    provider,
    claim_type,
    total_claims,
    unique_patients,
    total_reimbursed,
    total_deductible,
    avg_claim_amount,
    avg_reimbursed_to_deductible_ratio,
    avg_length_of_stay,
    post_death_claims,
    high_value_claims,
    potentialfraud
from {{ ref('int_provider_aggregates') }}
