with
    date_range as (

        select
            explode(
                sequence(to_date('2021-01-01'), to_date('2026-12-31'), interval 1 day)
            ) as full_date

    )

select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    date_format(full_date, 'MMMM') as month_name,
    weekofyear(full_date) as week_of_year,
    day(full_date) as day,
    date_format(full_date, 'EEEE') as day_name,

    case when dayofweek(full_date) in (1, 7) then true else false end as is_weekend

from date_range
