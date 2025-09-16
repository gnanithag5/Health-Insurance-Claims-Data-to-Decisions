{{ config(schema='INTERMEDIATE', materialized='table') }}


with base_claims as (

    -- Union inpatient and outpatient
    select
        'INPATIENT' as claim_type,
        claimid,
        beneid,
        provider,
        try_to_date(claimstartdt) as claim_start_date,
        try_to_date(claimenddt) as claim_end_date,
        try_to_date(admissiondt) as admission_date,
        try_to_date(dischargedt) as discharge_date,
        try_to_number(insclaimamtreimbursed) as reimbursed_amount,
        try_to_number(deductibleamtpaid) as deductible_paid,
        attendingphysician,
        operatingphysician,
        otherphysician
    from {{ ref('stg_inpatient') }}

    union all

    select
        'OUTPATIENT' as claim_type,
        claimid,
        beneid,
        provider,
        try_to_date(claimstartdt) as claim_start_date,
        try_to_date(claimenddt) as claim_end_date,
        null as admission_date,
        null as discharge_date,
        insclaimamtreimbursed::number as reimbursed_amount,
        deductibleamtpaid::number as deductible_paid,
        attendingphysician,
        operatingphysician,
        otherphysician
    from {{ ref('stg_outpatient') }}

),

with_bene as (

    select
        c.*,
        b.dob,
        b.dod,
        b.gender,
        b.race,
        datediff('year', try_to_date(b.dob), c.claim_start_date) as age_at_claim,
        case when c.claim_start_date > try_to_date(b.dod) then 1 else 0 end as is_post_death_claim,

        -- Chronic condition flags
        b.chroniccond_alzheimer,
        b.chroniccond_heartfailure,
        b.chroniccond_kidneydisease,
        b.chroniccond_cancer,
        b.chroniccond_obstrpulmonary,
        b.chroniccond_depression,
        b.chroniccond_diabetes,
        b.chroniccond_ischemicheart,
        b.chroniccond_osteoporasis,
        b.chroniccond_rheumatoidarthritis,
        b.chroniccond_stroke,

        b.state

    from base_claims c
    left join {{ ref('stg_beneficiary') }} b
      on c.beneid = b.beneid

),

with_provider as (

    select
        wb.*,
        p.potentialfraud,
        -- Provider-level derived ratios
        case 
            when wb.deductible_paid is not null and wb.reimbursed_amount is not null and wb.deductible_paid > 0
            then wb.reimbursed_amount / nullif(wb.deductible_paid,0)
        else null end as reimbursed_to_deductible_ratio,

        case 
            when wb.claim_end_date is not null and wb.claim_start_date is not null
            then datediff(day, wb.claim_start_date, wb.claim_end_date) + 1
        else null end as length_of_stay

    from with_bene wb
    left join {{ ref('stg_provider') }} p
      on wb.provider = p.provider

)

select * from with_provider
