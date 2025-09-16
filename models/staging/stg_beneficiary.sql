{{ config(schema='STAGING', materialized='table') }}

with base as (
    select
        nullif(trim(BeneID), '') as beneid,

        -- DD-MM-YYYY → proper dates
        coalesce(
          try_to_date(trim(DOB), 'YYYY-MM-DD'),
          try_to_date(trim(DOB), 'MM/DD/YYYY'),
          try_to_date(trim(DOB), 'DD-MM-YYYY')
          ) as dob,

        coalesce(
          try_to_date(trim(DOD), 'YYYY-MM-DD'),
          try_to_date(trim(DOD), 'MM/DD/YYYY'),
          try_to_date(trim(DOD), 'DD-MM-YYYY')
          ) as dod,


        try_to_number(nullif(trim(Gender), '')) as gender,
        try_to_number(nullif(trim(Race), ''))   as race,

        case upper(nullif(trim(RenalDiseaseIndicator), ''))
            when 'Y' then 1 else 0
        end as renalDiseaseIndicator,

        case
          when try_to_number(trim(State)) = 51 then 27
          when try_to_number(trim(State)) = 52 then 43
          when try_to_number(trim(State)) = 53 then 14
          when try_to_number(trim(State)) = 54 then 9
          else try_to_number(trim(State))
        end as state,
    
        try_to_number(nullif(trim(NoOfMonths_PartACov), '')) as noOfMonths_partACov,
        try_to_number(nullif(trim(NoOfMonths_PartBCov), '')) as noOfMonths_partBCov,

        try_to_number(nullif(trim(ChronicCond_Alzheimer), ''))            as chronicCond_Alzheimer,
        try_to_number(nullif(trim(ChronicCond_Heartfailure), ''))         as chronicCond_Heartfailure,
        try_to_number(nullif(trim(ChronicCond_KidneyDisease), ''))        as chronicCond_KidneyDisease,
        try_to_number(nullif(trim(ChronicCond_Cancer), ''))               as chronicCond_Cancer,
        try_to_number(nullif(trim(ChronicCond_ObstrPulmonary), ''))       as chronicCond_ObstrPulmonary,
        try_to_number(nullif(trim(ChronicCond_Depression), ''))           as chronicCond_Depression,
        try_to_number(nullif(trim(ChronicCond_Diabetes), ''))             as chronicCond_Diabetes,
        try_to_number(nullif(trim(ChronicCond_IschemicHeart), ''))        as chronicCond_IschemicHeart,
        try_to_number(nullif(trim(ChronicCond_Osteoporasis), ''))         as chronicCond_Osteoporasis,
        try_to_number(nullif(trim(ChronicCond_rheumatoidarthritis), ''))  as chronicCond_rheumatoidarthritis,
        try_to_number(nullif(trim(ChronicCond_stroke), ''))               as chronicCond_stroke,

        -- INR → USD (/88, rounded)
        round( try_to_number(nullif(trim(IPAnnualReimbursementAmt), '')) / 88 ) as IPAnnualReimbursementAmt,
        round( try_to_number(nullif(trim(IPAnnualDeductibleAmt),   '')) / 88 ) as IPAnnualDeductibleAmt,
        round( try_to_number(nullif(trim(OPAnnualReimbursementAmt), '')) / 88 ) as OPAnnualReimbursementAmt,
        round( try_to_number(nullif(trim(OPAnnualDeductibleAmt),   '')) / 88 ) as OPAnnualDeductibleAmt
    from {{ source('source_raw_data','beneficiary_raw') }}
),
filtered as (
    select *
    from base
    where beneid is not null
      and dob is not null
      and gender is not null
      and state is not null
)
select *
from filtered
qualify row_number() over (
    partition by beneid
    order by (dob is not null) desc, (dod is not null) desc
) = 1
