{{ config(schema='INTERMEDIATE', materialized='table') }}

with base_claims as (

    -- Inpatient claims
    select
        'INPATIENT' as claim_type,
        claimid,
        beneid,
        provider,
        
        coalesce(
          try_to_date(trim(ClaimStartDt), 'YYYY-MM-DD'),
          try_to_date(trim(ClaimStartDt), 'MM/DD/YYYY'),
          try_to_date(trim(ClaimStartDt), 'DD-MM-YYYY')
          ) as claim_start_date,

        coalesce(
          try_to_date(trim(ClaimEndDt), 'YYYY-MM-DD'),
          try_to_date(trim(ClaimEndDt), 'MM/DD/YYYY'),
          try_to_date(trim(ClaimEndDt), 'DD-MM-YYYY')
          ) as claim_end_date,
        
        coalesce(
          try_to_date(trim(AdmissionDt), 'YYYY-MM-DD'),
          try_to_date(trim(AdmissionDt), 'MM/DD/YYYY'),
          try_to_date(trim(AdmissionDt), 'DD-MM-YYYY')
          ) as admission_date,
        
        coalesce(
          try_to_date(trim(DischargeDt), 'YYYY-MM-DD'),
          try_to_date(trim(DischargeDt), 'MM/DD/YYYY'),
          try_to_date(trim(DischargeDt), 'DD-MM-YYYY')
          ) as discharge_date,
    
        try_to_number(insclaimamtreimbursed) as reimbursed_amount,
        try_to_number(deductibleamtpaid)    as deductible_paid,
        attendingphysician,
        operatingphysician,
        otherphysician
    from {{ ref('stg_inpatient') }}

    union all

    -- Outpatient claims
    select
        'OUTPATIENT' as claim_type,
        claimid,
        beneid,
        provider,
        coalesce(
          try_to_date(trim(ClaimStartDt), 'YYYY-MM-DD'),
          try_to_date(trim(ClaimStartDt), 'MM/DD/YYYY'),
          try_to_date(trim(ClaimStartDt), 'DD-MM-YYYY')
          ) as claim_start_date,

        coalesce(
          try_to_date(trim(ClaimEndDt), 'YYYY-MM-DD'),
          try_to_date(trim(ClaimEndDt), 'MM/DD/YYYY'),
          try_to_date(trim(ClaimEndDt), 'DD-MM-YYYY')
          ) as claim_end_date,

        null as admission_date,
        null as discharge_date,
        try_to_number(insclaimamtreimbursed) as reimbursed_amount,
        try_to_number(deductibleamtpaid)    as deductible_paid,
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
        datediff('year', try_to_date(b.dob), claim_start_date) as age_at_claim,
        case when b.dod is not null and claim_start_date > try_to_date(b.dod) then 1 else 0 end as is_post_death_claim,

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

        -- Length of stay
        case 
            when wb.discharge_date is not null and wb.admission_date is not null
            then datediff(day, wb.admission_date, wb.discharge_date) + 1
        else null end as length_of_stay

    from with_bene wb
    left join {{ ref('stg_provider') }} p
      on wb.provider = p.provider
)

select * 
from with_provider
