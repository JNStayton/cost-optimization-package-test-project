{{ config(
    materialized = 'table'
)}}

select
    1 as id,
    'rose' as flower_name,
    'red' as color,
    'tree' as plant_type,
    'flower' as new_col
