{{ config(schema='MARTS_FINANCE', materialized='table') }}

with calendar as (
    select
        dateadd(day, seq4(), '2008-11-01') as date_day
    from table(generator(rowcount => 425)) -- ~14 months of days (Nov 2008–Dec 2009)
)

select
    date_day,
    year(date_day)          as year,
    month(date_day)         as month,
    to_char(date_day,'Mon') as month_name,
    quarter(date_day)       as quarter,
    day(date_day)           as day,
    dayofweek(date_day)     as day_of_week
from calendar
