CREATE TABLE stock_prices (
  id serial PRIMARY KEY,   -- Auto-incrementing 'id'
  symbol text NOT NULL,     -- Stock symbol
  timestamp timestamp with time zone NOT NULL,  -- Timestamp for the price
  open double precision NOT NULL,  -- Opening price
  high double precision NOT NULL,  -- High price
  low double precision NOT NULL,   -- Low price
  close double precision NOT NULL, -- Closing price
  volume integer NOT NULL,  -- Volume of shares traded
  CONSTRAINT stock_prices_pkey PRIMARY KEY (symbol, timestamp)
);