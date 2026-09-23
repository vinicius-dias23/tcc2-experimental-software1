// Package model define as entidades do domínio (subconjunto do dataset Olist)
// e o envelope dos eventos de domínio publicados pelo Software A.
package model

import "time"

// Order espelha a tabela orders. As tags JSON coincidem com os nomes das colunas,
// de modo que a mesma struct decodifica tanto o evento de domínio quanto a linha
// capturada pelo Debezium (após o ExtractNewRecordState).
type Order struct {
	OrderID               string     `json:"order_id"`
	CustomerID            string     `json:"customer_id"`
	Status                string     `json:"order_status"`
	PurchaseTimestamp     *time.Time `json:"order_purchase_timestamp"`
	ApprovedAt            *time.Time `json:"order_approved_at"`
	DeliveredCarrierDate  *time.Time `json:"order_delivered_carrier_date"`
	DeliveredCustomerDate *time.Time `json:"order_delivered_customer_date"`
	EstimatedDeliveryDate *time.Time `json:"order_estimated_delivery_date"`
	Version               int64      `json:"version"`
	LastCorrelationID     *string    `json:"last_correlation_id"`
	UpdatedAt             *time.Time `json:"updated_at"`
}

// OrderItem espelha a tabela order_items. Valores monetários trafegam como string
// (decimal.handling.mode=string no Debezium) para não perder precisão.
type OrderItem struct {
	OrderID           string     `json:"order_id"`
	OrderItemID       int        `json:"order_item_id"`
	ProductID         *string    `json:"product_id"`
	SellerID          *string    `json:"seller_id"`
	ShippingLimitDate *time.Time `json:"shipping_limit_date"`
	Price             *string    `json:"price"`
	FreightValue      *string    `json:"freight_value"`
	CorrelationID     *string    `json:"correlation_id"`
}

// OrderPayment espelha a tabela order_payments.
type OrderPayment struct {
	OrderID             string  `json:"order_id"`
	PaymentSequential   int     `json:"payment_sequential"`
	PaymentType         *string `json:"payment_type"`
	PaymentInstallments *int    `json:"payment_installments"`
	PaymentValue        *string `json:"payment_value"`
	CorrelationID       *string `json:"correlation_id"`
}

// Tipos de evento de domínio emitidos pelo Software A.
const (
	EventOrderCreated       = "OrderCreated"
	EventOrderStatusChanged = "OrderStatusChanged"
)

// DomainEvent é o envelope publicado pelo order-service no Software A.
// Um evento por operação de negócio: OrderCreated carrega o agregado completo
// (pedido, itens e pagamentos); OrderStatusChanged carrega o pedido após a transição.
type DomainEvent struct {
	EventID       string         `json:"event_id"`
	EventType     string         `json:"event_type"`
	OccurredAt    time.Time      `json:"occurred_at"`
	CorrelationID string         `json:"correlation_id"`
	AggregateID   string         `json:"aggregate_id"`
	Version       int64          `json:"version"`
	Order         Order          `json:"order"`
	Items         []OrderItem    `json:"items,omitempty"`
	Payments      []OrderPayment `json:"payments,omitempty"`
}
