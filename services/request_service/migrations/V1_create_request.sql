CREATE TABLE IF NOT EXISTS requests (
    id          SERIAL PRIMARY KEY,
    client_id   VARCHAR(255) NOT NULL,
    product_id  VARCHAR(255) NOT NULL,
    quantity    INT NOT NULL CHECK (quantity > 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);