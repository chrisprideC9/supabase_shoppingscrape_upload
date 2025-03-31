-- Create scrape_types table
CREATE TABLE IF NOT EXISTS scrape_types (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert the two scrape types (if they don't exist already)
INSERT INTO scrape_types (name)
VALUES ('Products Scrape')
ON CONFLICT (name) DO NOTHING;

INSERT INTO scrape_types (name)
VALUES ('Shopping Tab Scrape')
ON CONFLICT (name) DO NOTHING;

-- Create scrape_data table to store all scrape information
CREATE TABLE IF NOT EXISTS scrape_data (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL,
    scrape_type_id INTEGER REFERENCES scrape_types(id),
    scrape_date TIMESTAMP WITH TIME ZONE NOT NULL,
    keyword VARCHAR(255),
    position INTEGER,
    product_id VARCHAR(255),
    title VARCHAR(1000),
    link TEXT,
    rating DECIMAL(3, 1),
    reviews INTEGER,
    price DECIMAL(10, 2),
    price_raw VARCHAR(50),
    merchant VARCHAR(255),
    is_carousel BOOLEAN,
    carousel_position VARCHAR(50),
    filters TEXT,
    has_product_page BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_scrape_data_campaign ON scrape_data(campaign_id);
CREATE INDEX IF NOT EXISTS idx_scrape_data_scrape_type ON scrape_data(scrape_type_id);
CREATE INDEX IF NOT EXISTS idx_scrape_data_keyword ON scrape_data(keyword);
CREATE INDEX IF NOT EXISTS idx_scrape_data_date ON scrape_data(scrape_date);

-- Create function to get campaigns with client info
CREATE OR REPLACE FUNCTION get_campaigns_with_clients()
RETURNS TABLE (
    campaign_id integer,
    client_id integer,
    domain_name text,
    brand_name text,
    created_at timestamp with time zone,
    clients json
) LANGUAGE sql AS $$
    SELECT 
        c.campaign_id,
        c.client_id,
        c.domain_name,
        c.brand_name,
        c.created_at,
        json_build_object(
            'client_id', cl.client_id,
            'name', cl.name,
            'surname', cl.surname,
            'email', cl.email
        ) as clients
    FROM campaigns c
    JOIN clients cl ON c.client_id = cl.client_id
    ORDER BY c.domain_name;
$$;