{{ config(schema='MARTS_FRAUD_ANALYSIS', materialized='table') }}

select
    claimid,
    beneid,
    provider,
    claim_type,
    diagnosis_code
from {{ ref('int_claims_diagnosis_long') }}
