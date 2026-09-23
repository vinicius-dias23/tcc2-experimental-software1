-- Banco de ORIGEM: pertence ao order-service (serviço de negócio).
-- O esquema é idêntico nos dois protótipos (Software A e Software B).
-- Nenhuma tabela de outbox existe aqui: o Software A publica direto na fila
-- e o Software B depende apenas do WAL (wal_level=logical, ver docker-compose.yml).

SET TIME ZONE 'UTC';

CREATE TABLE customers (
    customer_id              text PRIMARY KEY,
    customer_unique_id       text,
    customer_zip_code_prefix text,
    customer_city            text,
    customer_state           text
);

CREATE TABLE sellers (
    seller_id              text PRIMARY KEY,
    seller_zip_code_prefix text,
    seller_city            text,
    seller_state           text
);

CREATE TABLE products (
    product_id                 text PRIMARY KEY,
    product_category_name      text,
    product_name_length        integer,
    product_description_length integer,
    product_photos_qty         integer,
    product_weight_g           integer,
    product_length_cm          integer,
    product_height_cm          integer,
    product_width_cm           integer
);

-- version e last_correlation_id formam o oráculo do experimento:
-- cada operação de negócio incrementa version e grava o identificador de correlação
-- recebido na requisição, que chega ao destino tanto pelo evento de domínio quanto pela
-- linha capturada do WAL.
CREATE TABLE orders (
    order_id                      text PRIMARY KEY,
    customer_id                   text NOT NULL REFERENCES customers (customer_id),
    order_status                  text NOT NULL,
    order_purchase_timestamp      timestamptz,
    order_approved_at             timestamptz,
    order_delivered_carrier_date  timestamptz,
    order_delivered_customer_date timestamptz,
    order_estimated_delivery_date timestamptz,
    version                       bigint NOT NULL DEFAULT 1,
    last_correlation_id           uuid,
    updated_at                    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX orders_updated_at_idx ON orders (updated_at);

CREATE TABLE order_items (
    order_id            text NOT NULL REFERENCES orders (order_id),
    order_item_id       integer NOT NULL,
    product_id          text,
    seller_id           text,
    shipping_limit_date timestamptz,
    price               numeric(12, 2),
    freight_value       numeric(12, 2),
    correlation_id      uuid,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
    order_id             text NOT NULL REFERENCES orders (order_id),
    payment_sequential   integer NOT NULL,
    payment_type         text,
    payment_installments integer,
    payment_value        numeric(12, 2),
    correlation_id       uuid,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Registro da carga inicial (parcela do dataset pré-carregada).
CREATE TABLE seed_info (
    id             integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    seeded_at      timestamptz NOT NULL DEFAULT now(),
    seed_fraction  numeric NOT NULL,
    orders_total   integer NOT NULL,
    orders_seeded  integer NOT NULL,
    synthetic      boolean NOT NULL
);
