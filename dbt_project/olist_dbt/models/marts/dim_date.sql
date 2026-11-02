with days as (
    select generate_series('2016-09-01'::date, '2018-12-31'::date, interval '1 day')::date as date_day
)
select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    to_char(date_day, 'Mon') as month_name,
    extract(quarter from date_day)::int as quarter,
    extract(isodow from date_day)::int as day_of_week,
    to_char(date_day, 'Dy') as day_name,
    extract(isodow from date_day) in (6, 7) as is_weekend
from days
