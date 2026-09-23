-- Banco de DESTINO: pertence ao shipping-service (consumidor).
-- Materializa os pedidos a partir dos eventos recebidos, qualquer que seja o modelo
-- de propagação, e registra cada mensagem recebida em received_events.

SET TIME ZONE 'UTC';

CREATE TABLE orders (
    order_id                      text PRIMARY KEY,
    customer_id                   text,
    order_status                  text NOT NULL,
    order_purchase_timestamp      timestamptz,
    order_approved_at             timestamptz,
    order_delivered_carrier_date  timestamptz,
    order_delivered_customer_date timestamptz,
    order_estimated_delivery_date timestamptz,
    version                       bigint NOT NULL DEFAULT 1,
    last_correlation_id           uuid,
    updated_at                    timestamptz,
    applied_at                    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE order_items (
    order_id            text NOT NULL,
    order_item_id       integer NOT NULL,
    product_id          text,
    seller_id           text,
    shipping_limit_date timestamptz,
    price               numeric(12, 2),
    freight_value       numeric(12, 2),
    correlation_id      uuid,
    applied_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
    order_id             text NOT NULL,
    payment_sequential   integer NOT NULL,
    payment_type         text,
    payment_installments integer,
    payment_value        numeric(12, 2),
    correlation_id       uuid,
    applied_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (order_id, payment_sequential)
);

-- Uma linha por mensagem consumida, inclusive as duplicadas e as fora de ordem.
-- É daqui que saem perda, duplicação e violação de ordem.
--   source     : 'domain-events' ou 'cdc'
--   entity     : 'order', 'order_item' ou 'order_payment'
--   entity_key : chave da entidade (order_id, ou order_id/seq para itens e pagamentos)
--   applied    : se a mensagem alterou a projeção (false = descartada pela guarda de versão)
CREATE TABLE received_events (
    id              bigserial PRIMARY KEY,
    received_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    source          text NOT NULL,
    topic           text NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset    bigint NOT NULL,
    kafka_timestamp timestamptz,
    event_type      text NOT NULL,
    entity          text NOT NULL,
    order_id        text,
    entity_key      text,
    version         bigint,
    correlation_id  uuid,
    applied         boolean NOT NULL
);

CREATE INDEX received_events_correlation_idx ON received_events (correlation_id);
CREATE INDEX received_events_received_at_idx ON received_events (received_at);
