{{ config(schema='INTERMEDIATE', materialized='table') }}


with all_claims as (

    -- Combine inpatient & outpatient procedure columns
    select
        claimid,
        beneid,
        provider,
        'INPATIENT' as claim_type,
        clmprocedurecode_1,
        clmprocedurecode_2,
        clmprocedurecode_3,
        clmprocedurecode_4,
        clmprocedurecode_5,
        clmprocedurecode_6
    from {{ ref('stg_inpatient') }}

    union all

    select
        claimid,
        beneid,
        provider,
        'OUTPATIENT' as claim_type,
        clmprocedurecode_1,
        clmprocedurecode_2,
        clmprocedurecode_3,
        clmprocedurecode_4,
        clmprocedurecode_5,
        clmprocedurecode_6
    from {{ ref('stg_outpatient') }}

),

unpivoted as (

    select
        claimid,
        beneid,
        provider,
        claim_type,
        code as procedure_code
    from all_claims
    unpivot(code for col in (
        clmprocedurecode_1,
        clmprocedurecode_2,
        clmprocedurecode_3,
        clmprocedurecode_4,
        clmprocedurecode_5,
        clmprocedurecode_6
    ))
)

select *
from unpivoted
where procedure_code is not null
