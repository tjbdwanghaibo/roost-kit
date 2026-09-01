package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tjbdwanghaibo/cube-core/nats"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"log/slog"
	"strings"
)

// natsSyncBus implements fsync.ISyncBus over NATS pub/sub.
type natsSyncBus struct {
	client   nats.IClient
	localSid int32
	prefix   string // NATS subject prefix, e.g. "cube.sync"
}

func NewNatsSyncBus(client nats.IClient, localSid int32, prefix string) fsync.ISyncBus {
	if prefix == "" {
		prefix = "cube.sync"
	}
	return &natsSyncBus{client: client, localSid: localSid, prefix: prefix}
}

func (b *natsSyncBus) Publish(msg *fsync.SyncMsg) error {
	return b.PublishContext(context.Background(), msg)
}

func (b *natsSyncBus) PublishContext(ctx context.Context, msg *fsync.SyncMsg) error {
	if b == nil || b.client == nil {
		return fmt.Errorf("nats sync: bus is not initialized")
	}
	if msg == nil {
		return fmt.Errorf("nats sync: message is nil")
	}
	if strings.TrimSpace(msg.Topic) == "" {
		return fmt.Errorf("nats sync: topic is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owned := *msg
	owned.Data = append([]byte(nil), msg.Data...)
	if owned.FromSid == 0 {
		owned.FromSid = b.localSid
	}
	data, err := json.Marshal(&owned)
	if err != nil {
		return err
	}
	subject := fmt.Sprintf("%s.%s", b.prefix, owned.Topic)
	return b.client.Publish(subject, data)
}

func (b *natsSyncBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
	if b == nil || b.client == nil {
		return nil, fmt.Errorf("nats sync: bus is not initialized")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("nats sync: topic is empty")
	}
	if handler == nil {
		return nil, fmt.Errorf("nats sync: handler is nil")
	}
	subject := fmt.Sprintf("%s.%s", b.prefix, topic)
	sub, err := b.client.Subscribe(subject, func(m *nats.Msg) {
		var msg fsync.SyncMsg
		if err := json.Unmarshal(m.Data, &msg); err != nil {
			slog.Warn("sync: unmarshal failed", "topic", topic, "err", err)
			return
		}
		// Skip messages from self
		if msg.FromSid == b.localSid {
			return
		}
		if err := handler(&msg); err != nil {
			slog.Warn("sync: handler error", "topic", topic, "key", msg.Key, "err", err)
		}
	})
	if err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}
