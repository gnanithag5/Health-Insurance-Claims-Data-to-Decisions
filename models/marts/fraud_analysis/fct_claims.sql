{{ config(schema='MARTS_FRAUD_ANALYSIS', materialized='table') }}

with base as (

    select *
    from {{ ref('int_claims_enriched') }}
)

select
    c.claimid,
    c.beneid,
    c.provider,
    c.claim_type,
    c.claim_start_date,
    c.claim_end_date,
    c.admission_date,
    c.discharge_date,

    c.reimbursed_amount,
    c.deductible_paid,
    c.reimbursed_to_deductible_ratio,
    c.length_of_stay,

    c.age_at_claim,
    c.is_post_death_claim,
    c.potentialfraud,

    -- Human-readable demographic attributes
    g.gender as gender_label,
    r.race as race_label,
    s.state as state_name,

    -- Chronic conditions (mapped Yes/No from lookup)
    alz.flag   as alzheimer_flag,
    hf.flag    as heartfailure_flag,
    kd.flag    as kidneydisease_flag,
    ca.flag    as cancer_flag,
    copd.flag  as obstrpulmonary_flag,
    dep.flag   as depression_flag,
    diab.flag  as diabetes_flag,
    ih.flag    as ischemicheart_flag,
    osteo.flag as osteoporosis_flag,
    ra.flag    as rheumatoidarthritis_flag,
    str.flag   as stroke_flag

from base c
left join {{ ref('gender_lookup') }} g
  on c.gender = g.gender_id
left join {{ ref('race_lookup') }} r
  on c.race = r.race_id
left join {{ ref('state_lookup') }} s
  on c.state = s.state_id

-- Chronic conditions join for each column
left join {{ ref('yes_no_lookup') }} alz
  on c.chroniccond_alzheimer = alz.flag_id
left join {{ ref('yes_no_lookup') }} hf
  on c.chroniccond_heartfailure = hf.flag_id
left join {{ ref('yes_no_lookup') }} kd
  on c.chroniccond_kidneydisease = kd.flag_id
left join {{ ref('yes_no_lookup') }} ca
  on c.chroniccond_cancer = ca.flag_id
left join {{ ref('yes_no_lookup') }} copd
  on c.chroniccond_obstrpulmonary = copd.flag_id
left join {{ ref('yes_no_lookup') }} dep
  on c.chroniccond_depression = dep.flag_id
left join {{ ref('yes_no_lookup') }} diab
  on c.chroniccond_diabetes = diab.flag_id
left join {{ ref('yes_no_lookup') }} ih
  on c.chroniccond_ischemicheart = ih.flag_id
left join {{ ref('yes_no_lookup') }} osteo
  on c.chroniccond_osteoporasis = osteo.flag_id
left join {{ ref('yes_no_lookup') }} ra
  on c.chroniccond_rheumatoidarthritis = ra.flag_id
left join {{ ref('yes_no_lookup') }} str
  on c.chroniccond_stroke = str.flag_id
