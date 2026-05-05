{{ config(materialized='view') }}

select
    c.customer_key,
    c.customer_name,
    c.customer_address,
    c.nation_key,
    c.customer_phone_number,
    c.customer_account_balance,
    c.customer_market_segment,
    o.order_key,
    o.order_status,
    o.total_order_price,
    o.order_date,
    o.order_priority,
    o.order_clerk,
    o.order_ship_priority,
    o.order_comment
from {{ ref('stg_customers') }} c
inner join {{ ref('stg_orders') }} o
    on c.customer_key = o.customer_key