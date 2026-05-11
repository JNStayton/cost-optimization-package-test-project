{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select
    1 as id,
    'tulip' as flower_name,
    'yellow' as color,
    'tree' as plat_type

{% if is_incremental() %}
    where id > (select max(id) from {{ this }})
{% endif %}