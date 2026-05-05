{{ config(materialized='view') }}

select
    co.customer_key,
    co.customer_name,
    co.customer_address,
    co.nation_key,
    co.customer_phone_number,
    co.customer_account_balance,
    co.customer_market_segment,
    co.order_key,
    co.order_status,
    co.total_order_price,
    co.order_date,
    co.order_priority,
    co.order_clerk,
    co.order_ship_priority,
    li.part_key,
    li.supplier_key,
    li.line_number,
    li.quantity,
    li.extended_price,
    li.discount,
    li.tax,
    li.return_flag,
    li.line_status,
    li.ship_date,
    li.commit_date,
    li.receipt_date,
    li.ship_instruct,
    li.ship_mode
from {{ ref('int_customer_orders_vw') }} co
inner join {{ ref('stg_lineitem') }} li
    on co.order_key = li.order_key