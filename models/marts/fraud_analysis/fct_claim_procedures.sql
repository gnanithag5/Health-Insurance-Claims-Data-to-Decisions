{{ config(schema='MARTS_FRAUD_ANALYSIS', materialized='table') }}

select
    claimid,
    beneid,
    provider,
    claim_type,
    procedure_code
from {{ ref('int_claims_procedure_long') }}
