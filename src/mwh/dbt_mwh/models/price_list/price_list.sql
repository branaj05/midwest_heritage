{{
    config(
        materialized='table',
        database='MWH_ANALYTICS',
        schema='price_list'
    )
}}

SELECT
    pl.price_list_id,
    pl.description,
    c1.name AS category1,
    c2.name AS category2,
    c3.name AS category3,
    ps.name AS price_source,
    pl.quote_date AS quote_date,
    pl.dim_thickness*dim_width*dim_length/12/1000 as board_feet,
    pl.dim_thickness AS thickness,
    pl.dim_width AS width,
    pl.dim_length AS length,
    pl.notes,
    pl.price_value,
    uom.name AS unit_of_measure
FROM {{ source('price_list', 'price_list') }} pl
LEFT JOIN {{ source('price_list', 'price_categories') }} c1 ON pl.category1_id = c1.category_id
LEFT JOIN {{ source('price_list', 'price_categories') }} c2 ON pl.category2_id = c2.category_id
LEFT JOIN {{ source('price_list', 'price_categories') }} c3 ON pl.category3_id = c3.category_id
LEFT JOIN {{ source('price_list', 'price_source') }} ps ON pl.price_source_id = ps.price_source_id
LEFT JOIN {{ source('price_list', 'unit_of_measure') }} uom ON pl.unit_of_measure_id = uom.unit_of_measure_id
