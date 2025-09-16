{{ config(schema='MARTS_FINANCE', materialized='table') }}

with calendar as (
    select
        dateadd(day, seq4(), '2020-01-01') as date_day
    from table(generator(rowcount => 3650)) -- 10 years of days
)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    dayofweek(date_day) as day_of_week
from calendar
