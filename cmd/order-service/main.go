// order-service é o serviço de negócio (origem). Recebe as operações sobre pedidos,
// persiste no banco de origem e, apenas no modo domain-events (Software A), publica o
// evento de domínio diretamente na fila logo após o commit.
//
// No modo cdc (Software B) o código é o mesmo, mas a publicação é desligada: quem
// propaga é o Debezium, lendo o WAL do banco de origem.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/config"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/events"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/metrics"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/model"
	"github.com/vinicius-dias23/tcc2-experimental-software1/internal/pg"
)

const orderColumns = `order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
	order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
	version, last_correlation_id::text, updated_at`

type server struct {
	mode string
	db   *pgxpool.Pool
	pub  events.Publisher
	// Resposta ao cliente quando a publicação falha depois do commit:
	// "success" (padrão) responde 2xx, pois a mudança está commitada;
	// "error" responde 503, embora o commit já tenha ocorrido.
	failureResponse string
	// Tempo máximo das operações no banco por requisição; esgotado, a API responde 503.
	dbTimeout time.Duration

	created, statusChanged, httpErrors *atomic.Int64
	published, publishFailed           *atomic.Int64
	publishLatencyMs                   *atomic.Int64
	failuresSinceLog                   atomic.Int64
}

func main() {
	mode := config.Mode()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db := pg.Connect(ctx, config.String("DATABASE_URL", "postgres://olist:olist@postgres-origem:5432/olist"),
		int32(config.Int("DB_MAX_CONNS", 30)))
	defer db.Close()

	reg := metrics.New()
	s := &server{
		mode:             mode,
		db:               db,
		failureResponse:  config.String("PUBLISH_FAILURE_RESPONSE", "success"),
		dbTimeout:        config.Duration("DB_TIMEOUT", 10*time.Second),
		created:          reg.Counter("orders_created_total", "pedidos criados (commit concluído)"),
		statusChanged:    reg.Counter("order_status_changes_total", "transições de status commitadas"),
		httpErrors:       reg.Counter("http_errors_total", "requisições respondidas com erro"),
		published:        reg.Counter("events_published_total", "eventos de domínio confirmados pelo broker"),
		publishFailed:    reg.Counter("events_publish_failed_total", "eventos de domínio cuja publicação falhou após o commit"),
		publishLatencyMs: reg.Counter("events_publish_latency_ms_sum", "soma da latência de publicação (ms)"),
	}

	if mode == config.ModeDomainEvents {
		p := events.NewKafkaPublisher(events.KafkaPublisherConfig{
			Brokers:     config.List("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"),
			Topic:       config.String("DOMAIN_EVENTS_TOPIC", "olist.domain-events.orders"),
			MaxAttempts: config.Int("PUBLISH_MAX_ATTEMPTS", 10),
			Timeout:     config.Duration("PUBLISH_TIMEOUT", 5*time.Second),
		})
		s.pub = p
		go s.logPublishFailures(ctx)
	} else {
		s.pub = events.NoopPublisher{}
	}
	defer s.pub.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.createOrder)
	mux.HandleFunc("PATCH /orders/{id}/status", s.changeStatus)
	mux.HandleFunc("GET /orders/{id}", s.getOrder)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "mode": mode})
	})
	mux.Handle("GET /metrics", reg.Handler())

	addr := config.String("HTTP_ADDR", ":8080")
	httpSrv := &http.Server{Addr: addr, Handler: mux, ReadTimeout: 30 * time.Second, WriteTimeout: 120 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdown)
	}()
	log.Printf("order-service ouvindo em %s (modo %s)", addr, mode)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

type createOrderRequest struct {
	model.Order
	Items    []model.OrderItem    `json:"items"`
	Payments []model.OrderPayment `json:"payments"`
}

func (s *server) createOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.OrderID == "" || req.CustomerID == "" {
		s.fail(w, http.StatusBadRequest, "corpo inválido: order_id e customer_id são obrigatórios")
		return
	}
	if req.Status == "" {
		req.Status = "created"
	}
	corr := correlationID(r)
	ctx, cancel := context.WithTimeout(r.Context(), s.dbTimeout)
	defer cancel()

	tx, err := s.db.Begin(ctx)
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer tx.Rollback(context.Background())

	o := req.Order
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
			order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
			version, last_correlation_id, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, now())
		RETURNING version, last_correlation_id::text, updated_at`,
		o.OrderID, o.CustomerID, o.Status, o.PurchaseTimestamp, o.ApprovedAt,
		o.DeliveredCarrierDate, o.DeliveredCustomerDate, o.EstimatedDeliveryDate, corr,
	).Scan(&o.Version, &o.LastCorrelationID, &o.UpdatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			s.fail(w, http.StatusConflict, "pedido já existe")
			return
		}
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			s.fail(w, http.StatusUnprocessableEntity, "cliente inexistente")
			return
		}
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	batch := &pgx.Batch{}
	for i := range req.Items {
		it := &req.Items[i]
		it.OrderID, it.CorrelationID = o.OrderID, &corr
		batch.Queue(`INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date,
				price, freight_value, correlation_id)
			VALUES ($1, $2, $3, $4, $5, $6::text::numeric, $7::text::numeric, $8)`,
			it.OrderID, it.OrderItemID, it.ProductID, it.SellerID, it.ShippingLimitDate, it.Price, it.FreightValue, corr)
	}
	for i := range req.Payments {
		p := &req.Payments[i]
		p.OrderID, p.CorrelationID = o.OrderID, &corr
		batch.Queue(`INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments,
				payment_value, correlation_id)
			VALUES ($1, $2, $3, $4, $5::text::numeric, $6)`,
			p.OrderID, p.PaymentSequential, p.PaymentType, p.PaymentInstallments, p.PaymentValue, corr)
	}
	if batch.Len() > 0 {
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			s.fail(w, http.StatusUnprocessableEntity, err.Error())
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.created.Add(1)

	// A partir daqui a mudança está commitada. No Software A o evento é publicado
	// fora da transação: se a publicação falhar, nada registra que ele deveria existir.
	published := s.publish(r.Context(), model.DomainEvent{
		EventType: model.EventOrderCreated, CorrelationID: corr, AggregateID: o.OrderID, Version: o.Version,
		Order: o, Items: req.Items, Payments: req.Payments,
	})
	s.respondAfterCommit(w, http.StatusCreated, published, map[string]any{
		"order_id": o.OrderID, "version": o.Version, "correlation_id": corr,
	})
}

type changeStatusRequest struct {
	Status string `json:"order_status"`
	// Data da transição (approved, shipped e delivered preenchem a coluna de data
	// correspondente do Olist); se ausente, usa-se now().
	At *time.Time `json:"at"`
}

func (s *server) changeStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req changeStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Status == "" {
		s.fail(w, http.StatusBadRequest, "corpo inválido: order_status é obrigatório")
		return
	}
	corr := correlationID(r)
	ctx, cancel := context.WithTimeout(r.Context(), s.dbTimeout)
	defer cancel()

	var o model.Order
	err := s.db.QueryRow(ctx,
		`UPDATE orders SET
			order_status = $2, version = version + 1, last_correlation_id = $3, updated_at = now(),
			order_approved_at = CASE WHEN $2 = 'approved' THEN coalesce($4, now()) ELSE order_approved_at END,
			order_delivered_carrier_date = CASE WHEN $2 = 'shipped' THEN coalesce($4, now()) ELSE order_delivered_carrier_date END,
			order_delivered_customer_date = CASE WHEN $2 = 'delivered' THEN coalesce($4, now()) ELSE order_delivered_customer_date END
		WHERE order_id = $1 RETURNING `+orderColumns,
		id, req.Status, corr, req.At,
	).Scan(scanOrder(&o)...)
	if errors.Is(err, pgx.ErrNoRows) {
		s.fail(w, http.StatusNotFound, "pedido não encontrado")
		return
	}
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.statusChanged.Add(1)

	published := s.publish(r.Context(), model.DomainEvent{
		EventType: model.EventOrderStatusChanged, CorrelationID: corr, AggregateID: o.OrderID, Version: o.Version,
		Order: o,
	})
	s.respondAfterCommit(w, http.StatusOK, published, map[string]any{
		"order_id": o.OrderID, "version": o.Version, "order_status": o.Status, "correlation_id": corr,
	})
}

func (s *server) getOrder(w http.ResponseWriter, r *http.Request) {
	var o model.Order
	err := s.db.QueryRow(r.Context(), `SELECT `+orderColumns+` FROM orders WHERE order_id = $1`, r.PathValue("id")).
		Scan(scanOrder(&o)...)
	if errors.Is(err, pgx.ErrNoRows) {
		s.fail(w, http.StatusNotFound, "pedido não encontrado")
		return
	}
	if err != nil {
		s.fail(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, o)
}

// publish envia o evento no modo domain-events e devolve se ele foi confirmado.
// No modo cdc não faz nada e devolve true.
func (s *server) publish(ctx context.Context, ev model.DomainEvent) bool {
	if s.mode != config.ModeDomainEvents {
		return true
	}
	ev.EventID = uuid.NewString()
	ev.OccurredAt = time.Now().UTC()
	payload, err := json.Marshal(ev)
	if err != nil {
		log.Printf("erro serializando evento: %v", err)
		s.publishFailed.Add(1)
		return false
	}
	start := time.Now()
	// A publicação não é cancelada se o cliente desistir da requisição.
	err = s.pub.Publish(context.WithoutCancel(ctx), ev.AggregateID, payload, map[string]string{
		"correlation_id": ev.CorrelationID, "event_type": ev.EventType,
	})
	s.publishLatencyMs.Add(time.Since(start).Milliseconds())
	if err != nil {
		s.publishFailed.Add(1)
		s.failuresSinceLog.Add(1)
		return false
	}
	s.published.Add(1)
	return true
}

func (s *server) respondAfterCommit(w http.ResponseWriter, status int, published bool, body map[string]any) {
	if !published && s.failureResponse == "error" {
		s.fail(w, http.StatusServiceUnavailable, "mudança commitada, mas o evento não foi publicado")
		return
	}
	writeJSON(w, status, body)
}

// logPublishFailures resume as falhas de publicação a cada 5 s, em vez de uma linha
// por requisição, para não inundar o log durante janelas longas de indisponibilidade.
func (s *server) logPublishFailures(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := s.failuresSinceLog.Swap(0); n > 0 {
				log.Printf("ERRO: %d eventos não publicados nos últimos 5s (total %d)", n, s.publishFailed.Load())
			}
		}
	}
}

func (s *server) fail(w http.ResponseWriter, status int, msg string) {
	s.httpErrors.Add(1)
	writeJSON(w, status, map[string]string{"error": msg})
}

func scanOrder(o *model.Order) []any {
	return []any{&o.OrderID, &o.CustomerID, &o.Status, &o.PurchaseTimestamp, &o.ApprovedAt,
		&o.DeliveredCarrierDate, &o.DeliveredCustomerDate, &o.EstimatedDeliveryDate,
		&o.Version, &o.LastCorrelationID, &o.UpdatedAt}
}

func correlationID(r *http.Request) string {
	if v := r.Header.Get("X-Correlation-ID"); v != "" {
		if _, err := uuid.Parse(v); err == nil {
			return v
		}
	}
	return uuid.NewString()
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
