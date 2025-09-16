{{ config(schema='INTERMEDIATE', materialized='table') }}


with all_claims as (

    -- Combine inpatient & outpatient diagnosis columns
    select
        claimid,
        beneid,
        provider,
        'INPATIENT' as claim_type,
        clmdiagnoSIScode_1,
        clmdiagnoSIScode_2,
        clmdiagnoSIScode_3,
        clmdiagnoSIScode_4,
        clmdiagnoSIScode_5,
        clmdiagnoSIScode_6,
        clmdiagnoSIScode_7,
        clmdiagnoSIScode_8,
        clmdiagnoSIScode_9,
        clmdiagnoSIScode_10
    from {{ ref('stg_inpatient') }}

    union all

    select
        claimid,
        beneid,
        provider,
        'OUTPATIENT' as claim_type,
        clmdiagnoSIScode_1,
        clmdiagnoSIScode_2,
        clmdiagnoSIScode_3,
        clmdiagnoSIScode_4,
        clmdiagnoSIScode_5,
        clmdiagnoSIScode_6,
        clmdiagnoSIScode_7,
        clmdiagnoSIScode_8,
        clmdiagnoSIScode_9,
        clmdiagnoSIScode_10
    from {{ ref('stg_outpatient') }}

),

unpivoted as (

    select
        claimid,
        beneid,
        provider,
        claim_type,
        code as diagnosis_code
    from all_claims
    unpivot(code for col in (
        clmdiagnoSIScode_1,
        clmdiagnoSIScode_2,
        clmdiagnoSIScode_3,
        clmdiagnoSIScode_4,
        clmdiagnoSIScode_5,
        clmdiagnoSIScode_6,
        clmdiagnoSIScode_7,
        clmdiagnoSIScode_8,
        clmdiagnoSIScode_9,
        clmdiagnoSIScode_10
    ))
)

select *
from unpivoted
where diagnosis_code is not null
