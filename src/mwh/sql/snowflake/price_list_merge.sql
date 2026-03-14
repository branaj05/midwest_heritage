    MERGE INTO price_list.price_list AS target
    USING price_list.price_list_stage AS source
        ON  target.category1_id = source.category1_id
        AND target.category2_id = source.category2_id
        AND target.category3_id = source.category3_id
        AND target.quote_date = source.quote_date
        AND target.description = source.description
        AND EQUAL_NULL(target.price_source_id, source.price_source_id)
        AND EQUAL_NULL(target.unit_of_measure_id, source.unit_of_measure_id)
    WHEN MATCHED THEN UPDATE SET
        target.dim_thickness = source.dim_thickness,
        target.dim_width = source.dim_width,
        target.dim_length = source.dim_length,
        target.price_value = source.price_value,
        target.notes = source.notes,
        target.updated_at = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        category1_id, category2_id, category3_id, description,
        price_source_id, quote_date, dim_thickness, dim_width,
        dim_length, unit_of_measure_id, price_value, notes, updated_at
    ) VALUES (
        source.category1_id, source.category2_id, source.category3_id, source.description,
        source.price_source_id, source.quote_date, source.dim_thickness, source.dim_width,
        source.dim_length, source.unit_of_measure_id, source.price_value, source.notes, CURRENT_TIMESTAMP
    )