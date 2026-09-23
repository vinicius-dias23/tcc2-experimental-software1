// shipping-service é o consumidor (destino). Lê as mudanças de pedidos da fila e as
// materializa no banco de destino. A lógica de aplicação é a mesma nos dois protótipos;
// só muda o formato de entrada:
//
//   - domain-events: um evento por operação de negócio no tópico olist.domain-events.orders;
//   - cdc: uma mensagem por linha alterada nos tópicos olist.cdc.public.<tabela>,
//     já achatada pelo ExtractNewRecordState do Debezium.
//
// Cada mensagem recebida é registrada em received_events (inclusive duplicadas e fora de
// ordem) antes de ser aplicada, o que permite medir perda, duplicação e ordem depois.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/pg"
)

// change é a unidade que o consumidor aplica, independente do modelo de propagação.
type change struct {
	order    *model.Order
	items    []model.OrderItem
	payments []model.OrderPayment
}

// receipt descreve a mensagem para o registro em received_events.
type receipt struct {
	eventType     string
	entity        string
	orderID       string
	entityKey     string
	version       *int64
	correlationID *string
}

type consumer struct {
	mode string
	db   *pgxpool.Pool

	consumed, applied, invalid, dbErrors *atomic.Int64
}

func main() {
	mode := config.Mode()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workers := config.Int("CONSUMER_WORKERS", 3)
	db := pg.Connect(ctx, config.String("DATABASE_URL", "postgres://olist:olist@postgres-destino:5432/olist"),
		int32(workers+2))
	defer db.Close()

	defaultTopics := "olist.domain-events.orders"
	if mode == config.ModeCDC {
		defaultTopics = "olist.cdc.public.orders,olist.cdc.public.order_items,olist.cdc.public.order_payments"
	}
	topics := config.List("CONSUMER_TOPICS", defaultTopics)
	groupID := config.String("CONSUMER_GROUP_ID", "shipping-service-"+mode)
	brokers := config.List("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")

	reg := metrics.New()
	c := &consumer{
		mode:     mode,
		db:       db,
		consumed: reg.Counter("messages_consumed_total", "mensagens lidas da fila"),
		applied:  reg.Counter("messages_applied_total", "mensagens que alteraram a projeção"),
		invalid:  reg.Counter("messages_invalid_total", "mensagens que não puderam ser decodificadas"),
		dbErrors: reg.Counter("db_errors_total", "falhas ao gravar no banco de destino (com nova tentativa)"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, `{"status":"ok","mode":%q}`, mode)
	})
	mux.Handle("GET /metrics", reg.Handler())
	go func() {
		addr := config.String("HTTP_ADDR", ":8081")
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("shipping-service consumindo %v (grupo %s, %d workers, modo %s)", topics, groupID, workers, mode)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers:        brokers,
				GroupID:        groupID,
				GroupTopics:    topics,
				MinBytes:       1,
				MaxBytes:       10 << 20,
				MaxWait:        500 * time.Millisecond,
				StartOffset:    kafka.FirstOffset,
				CommitInterval: config.Duration("CONSUMER_COMMIT_INTERVAL", time.Second),
				SessionTimeout: config.Duration("CONSUMER_SESSION_TIMEOUT", 10*time.Second),
				ErrorLogger:    kafka.LoggerFunc(throttledLogger(fmt.Sprintf("worker %d: ", id))),
			})
			defer r.Close()
			c.run(ctx, r)
		}(i)
	}
	wg.Wait()
}

func (c *consumer) run(ctx context.Context, r *kafka.Reader) {
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("erro lendo a fila: %v", err)
			time.Sleep(time.Second)
			continue
		}
		c.consumed.Add(1)
		c.handle(ctx, msg)
		// Commit assíncrono (CommitInterval): entrega pelo menos uma vez.
		if err := r.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			log.Printf("erro registrando offset: %v", err)
		}
	}
}

func (c *consumer) handle(ctx context.Context, msg kafka.Message) {
	if len(msg.Value) == 0 {
		return // tombstone
	}
	ch, rc, err := c.decode(msg)
	if err != nil {
		c.invalid.Add(1)
		log.Printf("mensagem inválida em %s/%d@%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
		return
	}
	// Tenta até conseguir gravar: o offset só avança depois do registro no destino.
	for attempt := 0; ; attempt++ {
		err = c.apply(ctx, msg, ch, rc)
		if err == nil || ctx.Err() != nil {
			return
		}
		c.dbErrors.Add(1)
		if attempt%10 == 0 {
			log.Printf("erro gravando no destino (tentativa %d): %v", attempt+1, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (c *consumer) decode(msg kafka.Message) (change, receipt, error) {
	if c.mode == config.ModeDomainEvents {
		var ev model.DomainEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			return change{}, receipt{}, err
		}
		v, corr := ev.Version, ev.CorrelationID
		return change{order: &ev.Order, items: ev.Items, payments: ev.Payments},
			receipt{eventType: ev.EventType, entity: "order", orderID: ev.AggregateID, entityKey: ev.AggregateID,
				version: &v, correlationID: &corr}, nil
	}

	// CDC: a mensagem é a linha após a mudança, com os campos __op e __table
	// adicionados pelo ExtractNewRecordState.
	var meta struct {
		Op    string `json:"__op"`
		Table string `json:"__table"`
	}
	if err := json.Unmarshal(msg.Value, &meta); err != nil {
		return change{}, receipt{}, err
	}
	table := meta.Table
	if table == "" {
		table = msg.Topic[strings.LastIndex(msg.Topic, ".")+1:]
	}
	eventType := "row." + meta.Op
	switch table {
	case "orders":
		var o model.Order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			return change{}, receipt{}, err
		}
		v := o.Version
		return change{order: &o}, receipt{eventType: eventType, entity: "order", orderID: o.OrderID,
			entityKey: o.OrderID, version: &v, correlationID: o.LastCorrelationID}, nil
	case "order_items":
		var it model.OrderItem
		if err := json.Unmarshal(msg.Value, &it); err != nil {
			return change{}, receipt{}, err
		}
		return change{items: []model.OrderItem{it}}, receipt{eventType: eventType, entity: "order_item",
			orderID: it.OrderID, entityKey: fmt.Sprintf("%s/%d", it.OrderID, it.OrderItemID),
			correlationID: it.CorrelationID}, nil
	case "order_payments":
		var p model.OrderPayment
		if err := json.Unmarshal(msg.Value, &p); err != nil {
			return change{}, receipt{}, err
		}
		return change{payments: []model.OrderPayment{p}}, receipt{eventType: eventType, entity: "order_payment",
			orderID: p.OrderID, entityKey: fmt.Sprintf("%s/%d", p.OrderID, p.PaymentSequential),
			correlationID: p.CorrelationID}, nil
	}
	return change{}, receipt{}, errors.New("tabela desconhecida: " + table)
}

// apply grava a mudança e o recibo na mesma transação do banco de destino.
// O pedido só é sobrescrito por uma versão maior (guarda de versão), de modo que
// duplicatas e mensagens atrasadas ficam registradas mas não regridem a projeção.
func (c *consumer) apply(ctx context.Context, msg kafka.Message, ch change, rc receipt) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	applied := false
	if o := ch.order; o != nil {
		tag, err := tx.Exec(ctx, `
			INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
				order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
				version, last_correlation_id, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (order_id) DO UPDATE SET
				customer_id = EXCLUDED.customer_id, order_status = EXCLUDED.order_status,
				order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
				order_approved_at = EXCLUDED.order_approved_at,
				order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
				order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
				order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date,
				version = EXCLUDED.version, last_correlation_id = EXCLUDED.last_correlation_id,
				updated_at = EXCLUDED.updated_at, applied_at = clock_timestamp()
			WHERE orders.version < EXCLUDED.version`,
			o.OrderID, o.CustomerID, o.Status, o.PurchaseTimestamp, o.ApprovedAt, o.DeliveredCarrierDate,
			o.DeliveredCustomerDate, o.EstimatedDeliveryDate, o.Version, o.LastCorrelationID, o.UpdatedAt)
		if err != nil {
			return err
		}
		applied = tag.RowsAffected() > 0
	}
	for _, it := range ch.items {
		tag, err := tx.Exec(ctx, `
			INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date,
				price, freight_value, correlation_id)
			VALUES ($1, $2, $3, $4, $5, $6::text::numeric, $7::text::numeric, $8)
			ON CONFLICT (order_id, order_item_id) DO UPDATE SET
				product_id = EXCLUDED.product_id, seller_id = EXCLUDED.seller_id,
				shipping_limit_date = EXCLUDED.shipping_limit_date, price = EXCLUDED.price,
				freight_value = EXCLUDED.freight_value, correlation_id = EXCLUDED.correlation_id,
				applied_at = clock_timestamp()`,
			it.OrderID, it.OrderItemID, it.ProductID, it.SellerID, it.ShippingLimitDate, it.Price, it.FreightValue,
			it.CorrelationID)
		if err != nil {
			return err
		}
		applied = applied || tag.RowsAffected() > 0
	}
	for _, p := range ch.payments {
		tag, err := tx.Exec(ctx, `
			INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments,
				payment_value, correlation_id)
			VALUES ($1, $2, $3, $4, $5::text::numeric, $6)
			ON CONFLICT (order_id, payment_sequential) DO UPDATE SET
				payment_type = EXCLUDED.payment_type, payment_installments = EXCLUDED.payment_installments,
				payment_value = EXCLUDED.payment_value, correlation_id = EXCLUDED.correlation_id,
				applied_at = clock_timestamp()`,
			p.OrderID, p.PaymentSequential, p.PaymentType, p.PaymentInstallments, p.PaymentValue, p.CorrelationID)
		if err != nil {
			return err
		}
		applied = applied || tag.RowsAffected() > 0
	}

	var kafkaTS *time.Time
	if !msg.Time.IsZero() {
		t := msg.Time.UTC()
		kafkaTS = &t
	}
	source := c.mode
	if _, err := tx.Exec(ctx, `
		INSERT INTO received_events (source, topic, kafka_partition, kafka_offset, kafka_timestamp, event_type,
			entity, order_id, entity_key, version, correlation_id, applied)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		source, msg.Topic, msg.Partition, msg.Offset, kafkaTS, rc.eventType, rc.entity, rc.orderID, rc.entityKey,
		rc.version, rc.correlationID, applied); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if applied {
		c.applied.Add(1)
	}
	return nil
}

// throttledLogger evita inundar o log com a mesma falha de conexão enquanto os brokers
// estão fora: registra no máximo uma linha a cada 5 s por worker.
func throttledLogger(prefix string) func(string, ...interface{}) {
	var mu sync.Mutex
	var last time.Time
	var suppressed int
	return func(format string, args ...interface{}) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(last) < 5*time.Second {
			suppressed++
			return
		}
		msg := fmt.Sprintf(format, args...)
		if suppressed > 0 {
			msg += fmt.Sprintf(" (+%d mensagens suprimidas)", suppressed)
		}
		log.Print(prefix + msg)
		last, suppressed = time.Now(), 0
	}
}
