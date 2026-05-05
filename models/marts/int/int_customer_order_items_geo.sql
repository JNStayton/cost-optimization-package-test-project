{{ config(materialized='view') }}

select
    coi.customer_key,
    coi.customer_name,
    coi.customer_address,
    coi.customer_phone_number,
    coi.customer_account_balance,
    coi.customer_market_segment,
    coi.order_key,
    coi.order_status,
    coi.total_order_price,
    coi.order_date,
    coi.order_priority,
    coi.order_clerk,
    coi.order_ship_priority,
    coi.part_key,
    coi.supplier_key,
    coi.line_number,
    coi.quantity,
    coi.extended_price,
    coi.discount,
    coi.tax,
    coi.return_flag,
    coi.line_status,
    coi.ship_date,
    coi.commit_date,
    coi.receipt_date,
    coi.ship_instruct,
    coi.ship_mode,
    n.nation,
    n.region_key,
    r.region
from {{ ref('int_customer_order_items') }} coi
inner join {{ ref('stg_nations') }} n
    on coi.nation_key = n.nation_key
inner join {{ ref('stg_regions') }} r
    on n.region_key = r.region_key