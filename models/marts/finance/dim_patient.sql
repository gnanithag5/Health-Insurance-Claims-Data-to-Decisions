{{ config(schema='MARTS_FINANCE', materialized='table') }}

with base as (
    select
        b.beneid,
        b.dob,
        b.dod,
        g.gender as gender_label,
        s.state as state_name,

        -- Chronic condition flags
        alz.flag as alzheimer_flag,
        hf.flag  as heartfailure_flag,
        kd.flag  as kidneydisease_flag,
        ca.flag  as cancer_flag,
        copd.flag as obstrpulmonary_flag,
        dep.flag  as depression_flag,
        diab.flag as diabetes_flag,
        ih.flag   as ischemicheart_flag,
        osteo.flag as osteoporosis_flag,
        ra.flag   as rheumatoidarthritis_flag,
        str.flag  as stroke_flag
    from {{ ref('stg_beneficiary') }} b
    left join {{ ref('gender_lookup') }} g on b.gender = g.gender_id
    left join {{ ref('state_lookup') }} s on b.state = s.state_id
    left join {{ ref('yes_no_lookup') }} alz on b.chroniccond_alzheimer = alz.flag_id
    left join {{ ref('yes_no_lookup') }} hf  on b.chroniccond_heartfailure = hf.flag_id
    left join {{ ref('yes_no_lookup') }} kd  on b.chroniccond_kidneydisease = kd.flag_id
    left join {{ ref('yes_no_lookup') }} ca  on b.chroniccond_cancer = ca.flag_id
    left join {{ ref('yes_no_lookup') }} copd on b.chroniccond_obstrpulmonary = copd.flag_id
    left join {{ ref('yes_no_lookup') }} dep  on b.chroniccond_depression = dep.flag_id
    left join {{ ref('yes_no_lookup') }} diab on b.chroniccond_diabetes = diab.flag_id
    left join {{ ref('yes_no_lookup') }} ih   on b.chroniccond_ischemicheart = ih.flag_id
    left join {{ ref('yes_no_lookup') }} osteo on b.chroniccond_osteoporasis = osteo.flag_id
    left join {{ ref('yes_no_lookup') }} ra    on b.chroniccond_rheumatoidarthritis = ra.flag_id
    left join {{ ref('yes_no_lookup') }} str   on b.chroniccond_stroke = str.flag_id
)
select * from base
