{{
    config(
        materialized='table',
        database='MWH_ANALYTICS',
        schema='price_list_price_list'
    )
}}

with latest_prices as (
    select
        price_list_id,
        description,
        category1,
        category2,
        category3,
        price_source,
        quote_date,
        board_feet,
        thickness,
        width,
        length,
        notes,
        price_value,
        unit_of_measure,
        row_number() over (
            partition by description, category1, category2, category3,
                        thickness, width, length, unit_of_measure
            order by quote_date desc
        ) as rn
    from {{ ref('price_list') }}
)

select * from latest_prices where rn = 1