// Package events contém a publicação direta na fila usada pelo Software A.
package events

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Publisher publica um evento de domínio na fila.
type Publisher interface {
	Publish(ctx context.Context, key string, value []byte, headers map[string]string) error
	Close() error
}

// NoopPublisher é usado no Software B: o serviço de negócio não publica nada,
// a propagação fica a cargo do Debezium.
type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, string, []byte, map[string]string) error { return nil }
func (NoopPublisher) Close() error                                                     { return nil }

// KafkaPublisherConfig parametriza o produtor síncrono do Software A.
type KafkaPublisherConfig struct {
	Brokers     []string
	Topic       string
	MaxAttempts int
	// Timeout é o tempo máximo que a requisição de negócio espera pela confirmação
	// do broker. Esgotado esse tempo, a publicação é dada como falha e o evento se
	// perde: não há registro durável de que ele deveria existir (sem Outbox).
	Timeout time.Duration
}

type KafkaPublisher struct {
	w       *kafka.Writer
	timeout time.Duration
}

func NewKafkaPublisher(cfg KafkaPublisherConfig) *KafkaPublisher {
	return &KafkaPublisher{
		timeout: cfg.Timeout,
		w: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Topic:    cfg.Topic,
			Balancer: kafka.Murmur2Balancer{}, // mesmo particionamento por chave dos clientes Java
			// acks=all com min.insync.replicas=2 nos brokers.
			RequiredAcks:           kafka.RequireAll,
			MaxAttempts:            cfg.MaxAttempts,
			WriteBackoffMin:        100 * time.Millisecond,
			WriteBackoffMax:        time.Second,
			BatchTimeout:           5 * time.Millisecond,
			WriteTimeout:           10 * time.Second,
			ReadTimeout:            10 * time.Second,
			AllowAutoTopicCreation: false,
		},
	}
}

// Publish é síncrono: bloqueia o caminho da requisição até o broker confirmar
// ou até o timeout configurado.
func (p *KafkaPublisher) Publish(ctx context.Context, key string, value []byte, headers map[string]string) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	msg := kafka.Message{Key: []byte(key), Value: value}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}
	return p.w.WriteMessages(ctx, msg)
}

func (p *KafkaPublisher) Close() error { return p.w.Close() }
