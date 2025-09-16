{{ config(schema='INTERMEDIATE', materialized='table') }}


with patient_claims as (

    select
        beneid,
        count(distinct claimid) as total_claims,
        count(distinct provider) as unique_providers,
        sum(reimbursed_amount) as total_reimbursed,
        sum(deductible_paid) as total_deductible,
        avg(reimbursed_amount) as avg_claim_amount,

        -- Fraud-related signals
        sum(case when is_post_death_claim = 1 then 1 else 0 end) as post_death_claims,
        sum(case when reimbursed_amount > 10000 then 1 else 0 end) as high_value_claims

    from {{ ref('int_claims_enriched') }}
    group by beneid

),

with_bene_info as (

    select
        pc.*,
        b.gender,
        datediff(year, try_to_date(b.dob), current_date) as current_age,
        case when b.dod is not null then 1 else 0 end as is_deceased,

        -- Chronic condition counts
        (
            coalesce(b.chroniccond_alzheimer,0) +
            coalesce(b.chroniccond_heartfailure,0) +
            coalesce(b.chroniccond_kidneydisease,0) +
            coalesce(b.chroniccond_cancer,0) +
            coalesce(b.chroniccond_obstrpulmonary,0) +
            coalesce(b.chroniccond_depression,0) +
            coalesce(b.chroniccond_diabetes,0) +
            coalesce(b.chroniccond_ischemicheart,0) +
            coalesce(b.chroniccond_osteoporasis,0) +
            coalesce(b.chroniccond_rheumatoidarthritis,0) +
            coalesce(b.chroniccond_stroke,0)
        ) as chronic_condition_count

    from patient_claims pc
    left join {{ ref('stg_beneficiary') }} b
      on pc.beneid = b.beneid

)

select * from with_bene_info
