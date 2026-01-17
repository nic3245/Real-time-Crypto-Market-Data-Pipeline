CREATE TABLE raw_price_events (
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(18, 8) NOT NULL, -- 18 digits total, 8 decimal places
    source VARCHAR(50),
    PRIMARY KEY (symbol, timestamp) -- Ensure no dupe events
);