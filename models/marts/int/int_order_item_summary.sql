{{ config(materialized='ephemeral') }}

with order_items_enriched as (
    select
        *,
        extended_price * (1 - discount) as discounted_price,
        extended_price * (1 - discount) * (1 + tax) as charge_amount,
        datediff('day', order_date, ship_date) as days_to_ship,
        datediff('day', ship_date, receipt_date) as days_in_transit
    from {{ ref('int_customer_order_items_geo') }}
),

customer_running_totals as (
    select
        *,
        sum(discounted_price) over (
            partition by customer_key
            order by order_date, order_key, line_number
            rows unbounded preceding
        ) as customer_running_revenue,
        row_number() over (
            partition by customer_key
            order by order_date, order_key, line_number
        ) as customer_line_item_seq,
        avg(discounted_price) over (
            partition by customer_key, region
            order by order_date
            rows between 10 preceding and current row
        ) as rolling_avg_revenue_by_region
    from order_items_enriched
),

ranked as (
    select
        *,
        dense_rank() over (
            partition by region, order_date
            order by charge_amount desc
        ) as daily_region_rank
    from customer_running_totals
)

select * from ranked