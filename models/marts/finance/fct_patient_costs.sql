{{ config(schema='MARTS_FINANCE', materialized='table') }}

select
    beneid,
    total_claims,
    unique_providers,
    total_reimbursed,
    total_deductible,
    avg_claim_amount,
    chronic_condition_count
from {{ ref('int_patient_aggregates') }}
