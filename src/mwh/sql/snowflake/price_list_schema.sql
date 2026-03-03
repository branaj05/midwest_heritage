-- Create price list Schema
CREATE SCHEMA IF NOT EXISTS price_list;
-- DROP SCHEMA IF EXISTS price_list CASCADE;
-- CREATE SCHEMA price_list;

-- Create price list tables inside the price_list schema
--- Create Price List Supporting Tables
CREATE TABLE IF NOT EXISTS price_list.price_categories(
    category_id INT AUTOINCREMENT PRIMARY KEY,
    name TEXT NOT NULL,
    level TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_price_categories_name_level UNIQUE (name, level)
);

CREATE TABLE IF NOT EXISTS price_list.price_source(
    price_source_id INT AUTOINCREMENT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_list.unit_of_measure(
    unit_of_measure_id INT AUTOINCREMENT PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    description TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


--- Create Price List Master Table
CREATE TABLE IF NOT EXISTS price_list.price_list(
    price_list_id BIGINT AUTOINCREMENT PRIMARY KEY,

    category1_id INT 
        REFERENCES price_list.price_categories(category_id),
    category2_id INT 
        REFERENCES price_list.price_categories(category_id),
    category3_id 
        INT REFERENCES price_list.price_categories(category_id),

    description TEXT,

    -- quote_type_id INT 
    --     REFERENCES price_list.quote_type(quote_type_id),
    price_source_id INT 
        REFERENCES price_list.price_source(price_source_id),

    quote_date DATE NOT NULL,

    dim_thickness DECIMAL(8,3),
    dim_width DECIMAL(8,3),
    dim_length DECIMAL(8,3),

    unit_of_measure_id INT 
        REFERENCES price_list.unit_of_measure(unit_of_measure_id),

    price_value DECIMAL(12,2) NOT NULL,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT price_list_natural_key UNIQUE (
    category1_id,
    category2_id,
    category3_id,
    description,
    quote_date,
    price_source_id)
    
);


--- Create Price List Import Log Table
CREATE TABLE IF NOT EXISTS price_list.import_log(
    import_id BIGINT AUTOINCREMENT PRIMARY KEY,
    import_file_name TEXT NOT NULL,
    import_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    imported_by TEXT,
    records_processed INT,
    records_successful INT,
    records_failed INT,
    notes TEXT
);

--- Create Price List Import Errors Table
CREATE TABLE IF NOT EXISTS price_list.import_errors(
    error_id BIGINT AUTOINCREMENT PRIMARY KEY,
    import_id BIGINT 
        REFERENCES price_list.import_log(import_id),
    row_number INT,
    error_message TEXT,
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);





