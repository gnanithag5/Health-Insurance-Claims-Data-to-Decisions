{{ config(schema='MARTS_FINANCE', materialized='table') }}

select
    provider,
    claim_type,
    total_claims,
    unique_patients,
    total_reimbursed,
    total_deductible,
    avg_claim_amount
from {{ ref('int_provider_aggregates') }}
