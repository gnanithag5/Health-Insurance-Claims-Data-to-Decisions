{{ config(schema='STAGING', materialized='table') }}

with base as (
    select
        nullif(trim(ClaimID), '')  as claimid,
        nullif(trim(BeneID),  '')  as beneid,
        nullif(trim(Provider), '') as provider,

        try_to_date(nullif(trim(ClaimStartDt), ''), 'DD-MM-YYYY') as claimstartdt,
        try_to_date(nullif(trim(ClaimEndDt),   ''), 'DD-MM-YYYY') as claimenddt,

        round(try_to_number(nullif(trim(InsClaimAmtReimbursed), '')) / 88) as insclaimamtreimbursed,
        round(try_to_number(nullif(trim(DeductibleAmtPaid),       '')) / 88) as deductibleamtpaid,

        upper(trim(ClmAdmitDiagnosisCode)) as clmadmitdiagnosiscode,

        upper(trim(ClmDiagnosisCode_1))  as clmdiagnoSIScode_1,
        upper(trim(ClmDiagnosisCode_2))  as clmdiagnoSIScode_2,
        upper(trim(ClmDiagnosisCode_3))  as clmdiagnoSIScode_3,
        upper(trim(ClmDiagnosisCode_4))  as clmdiagnoSIScode_4,
        upper(trim(ClmDiagnosisCode_5))  as clmdiagnoSIScode_5,
        upper(trim(ClmDiagnosisCode_6))  as clmdiagnoSIScode_6,
        upper(trim(ClmDiagnosisCode_7))  as clmdiagnoSIScode_7,
        upper(trim(ClmDiagnosisCode_8))  as clmdiagnoSIScode_8,
        upper(trim(ClmDiagnosisCode_9))  as clmdiagnoSIScode_9,
        upper(trim(ClmDiagnosisCode_10)) as clmdiagnoSIScode_10,

        upper(trim(ClmProcedureCode_1))  as clmprocedurecode_1,
        upper(trim(ClmProcedureCode_2))  as clmprocedurecode_2,
        upper(trim(ClmProcedureCode_3))  as clmprocedurecode_3,
        upper(trim(ClmProcedureCode_4))  as clmprocedurecode_4,
        upper(trim(ClmProcedureCode_5))  as clmprocedurecode_5,
        upper(trim(ClmProcedureCode_6))  as clmprocedurecode_6,

        upper(trim(AttendingPhysician))  as attendingphysician,
        upper(trim(OperatingPhysician))  as operatingphysician,
        upper(trim(OtherPhysician))      as otherphysician,

        'OUTPATIENT' as claim_type
    from {{ source('source_raw_data','outpatient_raw') }}
),
filtered as (
    select *
    from base
    where claimid is not null
      and beneid is not null
      and provider is not null
)
select *
from filtered
qualify row_number() over (
    partition by claimid
    order by
        (claimenddt is not null) desc,
        (insclaimamtreimbursed is not null) desc,
        insclaimamtreimbursed desc
) = 1
