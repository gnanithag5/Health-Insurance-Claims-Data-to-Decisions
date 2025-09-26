with claim_level as (
    select
        c.beneid,
        c.claimid,
        c.reimbursed_amount,
        c.deductible_paid,
        -- Age at the time of claim
        datediff('year', try_to_date(b.dob), c.claim_start_date) as age_at_claim,
        -- Bucket into 10-year groups
        floor(datediff('year', try_to_date(b.dob), c.claim_start_date) / 10) * 10 
            || '-' ||
        (floor(datediff('year', try_to_date(b.dob), c.claim_start_date) / 10) * 10 + 9) as age_group
    from {{ ref('int_claims_enriched') }} c
    left join {{ ref('stg_beneficiary') }} b
      on c.beneid = b.beneid
),

agg as (
    select
        c.beneid,
        age_group,
        count(distinct c.claimid) as total_claims,
        sum(c.reimbursed_amount) as total_reimbursed,
        sum(c.deductible_paid) as total_deductible,
        avg(c.reimbursed_amount) as avg_claim_amount
    from claim_level c
    group by c.beneid, age_group
)

select * from agg
