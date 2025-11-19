with european_customers as (
    select
        c.customer_key,
        c.customer_market_segment
    from
        {{ ref('stg_customers') }} c
    join
        {{ ref('stg_nations') }} n on c.nation_key = n.nation_key
    join
        {{ ref('stg_regions') }} r on n.region_key = r.region_key
    -- filter as early as possible
    where
        r.region = 'EUROPE'
    and c.customer_market_segment = 'BUILDING'
),

late_order_items as (
    select
        customer_key,
        order_date,
        net_item_sales_amount
    from
        {{ ref('int_order_items') }}
    where
        -- filter early and leveraging the clustering key
        receipt_date > commit_date
        and order_date >= '1995-01-01'
)

-- final select statement 
select
    extract(quarter from loi.order_date) as order_quarter,
    ec.customer_market_segment,
    sum(loi.net_item_sales_amount) as total_revenue
from
    late_order_items loi
join
    european_customers ec on loi.customer_key = ec.customer_key
group by
    1, 2
order by
    1, 2