-- Case 2: acquisition cohorts by first-order month. table. Built on dim_customers
-- (customer_unique_id).
{{ config(materialized='table') }}
with customers as (select * from {{ ref('dim_customers') }})
select
    date_trunc('month', customers.first_order_ts) as cohort_month,
    count(*) as n_customers,
    coalesce(sum(customers.is_repeat_customer::int), 0) as n_repeat_customers,
    round(avg(customers.is_repeat_customer::int) * 100, 2) as repeat_customer_pct,
    round(avg(customers.lifetime_value), 2) as avg_lifetime_value
from customers
group by cohort_month
order by cohort_month
