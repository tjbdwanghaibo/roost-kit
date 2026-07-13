package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	fnats "github.com/tjbdwanghaibo/cube-core/nats"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
)

const (
	defaultJetStreamSyncPrefix       = "cube.sync"
	defaultJetStreamSyncStream       = "CUBE_SYNC"
	defaultJetStreamSyncAckWait      = 10 * time.Second
	defaultJetStreamSyncMaxDeliver   = 5
	defaultJetStreamSyncMaxAge       = 30 * time.Minute
	defaultJetStreamSyncDuplicates   = 2 * time.Minute
	defaultJetStreamSyncSetupTimeout = 5 * time.Second
	defaultJetStreamSyncPublishTime  = 5 * time.Second
)

type JetStreamSyncConfig struct {
	LocalSid     int32
	Prefix       string
	Stream       string
	Storage      fnats.JetStreamStorage
	AckWait      time.Duration
	MaxDeliver   int
	StreamMaxAge time.Duration
	Duplicates   time.Duration
	Replicas     int
	MaxBytes     int64
	SetupTimeout time.Duration
	PublishTime  time.Duration
}

func normalizeJetStreamSyncConfig(cfg JetStreamSyncConfig) JetStreamSyncConfig {
	if cfg.Prefix == "" {
		cfg.Prefix = defaultJetStreamSyncPrefix
	}
	if cfg.Stream == "" {
		cfg.Stream = defaultJetStreamSyncStream
	}
	if cfg.Storage == "" {
		cfg.Storage = fnats.JetStreamStorageFile
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = defaultJetStreamSyncAckWait
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = defaultJetStreamSyncMaxDeliver
	}
	if cfg.StreamMaxAge <= 0 {
		cfg.StreamMaxAge = defaultJetStreamSyncMaxAge
	}
	if cfg.Duplicates <= 0 {
		cfg.Duplicates = defaultJetStreamSyncDuplicates
	}
	if cfg.SetupTimeout <= 0 {
		cfg.SetupTimeout = defaultJetStreamSyncSetupTimeout
	}
	if cfg.PublishTime <= 0 {
		cfg.PublishTime = defaultJetStreamSyncPublishTime
	}
	return cfg
}

type jetStreamSyncBus struct {
	js  fnats.IJetStream
	cfg JetStreamSyncConfig

	mu   sync.Mutex
	subs []fnats.IJetStreamSubscription
}

func NewJetStreamSyncBus(ctx context.Context, js fnats.IJetStream, cfg JetStreamSyncConfig) (*jetStreamSyncBus, error) {
	if js == nil {
		return nil, fmt.Errorf("jetstream sync: jetstream is nil")
	}
	cfg = normalizeJetStreamSyncConfig(cfg)
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	setupCtx, cancel := context.WithTimeout(ctx, cfg.SetupTimeout)
	defer cancel()
	if err := js.EnsureStream(setupCtx, fnats.JetStreamConfig{
		Name:       cfg.Stream,
		Subjects:   []string{fmt.Sprintf("%s.>", cfg.Prefix)},
		Storage:    cfg.Storage,
		MaxAge:     cfg.StreamMaxAge,
		Duplicates: cfg.Duplicates,
		Replicas:   cfg.Replicas,
		MaxBytes:   cfg.MaxBytes,
	}); err != nil {
		return nil, fmt.Errorf("jetstream sync: ensure stream: %w", err)
	}
	return &jetStreamSyncBus{js: js, cfg: cfg}, nil
}

func (b *jetStreamSyncBus) Publish(msg *fsync.SyncMsg) error {
	if b == nil || b.js == nil || msg == nil {
		return nil
	}
	if strings.TrimSpace(msg.Topic) == "" {
		return fmt.Errorf("jetstream sync: topic is empty")
	}
	if msg.FromSid == 0 {
		msg.FromSid = b.cfg.LocalSid
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(fctx.BaseContext(), b.cfg.PublishTime)
	defer cancel()
	_, err = b.js.Publish(ctx, b.subject(msg.Topic), data, fnats.JetStreamPublishOptions{MsgID: syncMsgID(msg)})
	return err
}

func (b *jetStreamSyncBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
	if b == nil || b.js == nil || strings.TrimSpace(topic) == "" || handler == nil {
		return func() {}, nil
	}
	cfg := b.cfg
	name := sanitizeSyncName(fmt.Sprintf("sync_%s_%d", topic, cfg.LocalSid))
	ctx, cancel := context.WithTimeout(fctx.BaseContext(), cfg.SetupTimeout)
	defer cancel()
	sub, err := b.js.Subscribe(ctx, fnats.JetStreamConsumerConfig{
		Stream:        cfg.Stream,
		Name:          name,
		Durable:       name,
		FilterSubject: b.subject(topic),
		DeliverPolicy: fnats.JetStreamDeliverAll,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
	}, func(_ context.Context, raw *fnats.JetStreamMsg) error {
		if raw == nil {
			return nil
		}
		var msg fsync.SyncMsg
		if err := json.Unmarshal(raw.Data, &msg); err != nil {
			slog.Warn("jetstream sync: unmarshal failed", "topic", topic, "err", err)
			return err
		}
		if msg.FromSid == cfg.LocalSid {
			return nil
		}
		if err := handler(&msg); err != nil {
			slog.Warn("jetstream sync: handler error", "topic", topic, "key", msg.Key, "version", msg.Version, "err", err)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	return func() {
		sub.Stop()
	}, nil
}

func (b *jetStreamSyncBus) Stop() {
	if b == nil {
		return
	}
	b.mu.Lock()
	subs := append([]fnats.IJetStreamSubscription(nil), b.subs...)
	b.subs = nil
	b.mu.Unlock()
	for _, sub := range subs {
		if sub != nil {
			sub.Stop()
		}
	}
}

func (b *jetStreamSyncBus) subject(topic string) string {
	return fmt.Sprintf("%s.%s", b.cfg.Prefix, topic)
}

func syncMsgID(msg *fsync.SyncMsg) string {
	if msg == nil || msg.Topic == "" || msg.Key == 0 || msg.Version == 0 || msg.FromSid == 0 {
		return ""
	}
	return fmt.Sprintf("sync:%s:%d:%d:%d", msg.Topic, msg.Key, msg.Version, msg.FromSid)
}

func sanitizeSyncName(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	b.Grow(len(s))
	lastUnderscore := false
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
		if ok {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if len(out) > 200 {
		out = out[:200]
	}
	if out == "" {
		return "sync"
	}
	return out
}

var _ fsync.ISyncBus = (*jetStreamSyncBus)(nil)
