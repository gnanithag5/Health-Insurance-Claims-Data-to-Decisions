{{ config(schema='INTERMEDIATE', materialized='table') }}


with provider_claims as (

    select
        provider,
        claim_type,
        count(distinct claimid) as total_claims,
        count(distinct beneid) as unique_patients,
        sum(reimbursed_amount) as total_reimbursed,
        sum(deductible_paid) as total_deductible,
        avg(reimbursed_amount) as avg_claim_amount,
        avg(length_of_stay) as avg_length_of_stay,

        -- Fraud-related signals
        sum(case when is_post_death_claim = 1 then 1 else 0 end) as post_death_claims,
        sum(case when reimbursed_amount > 10000 then 1 else 0 end) as high_value_claims

    from {{ ref('int_claims_enriched') }}
    group by provider, claim_type

),

with_flags as (

    select
        pc.*,
        p.potentialfraud
    from provider_claims pc
    left join {{ ref('stg_provider') }} p
      on pc.provider = p.provider

)

select * from with_flags
