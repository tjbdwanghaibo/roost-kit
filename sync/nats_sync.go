package sync

import (
	"encoding/json"
	"fmt"
	"github.com/tjbdwanghaibo/cube-core/nats"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"log/slog"
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
	if msg.FromSid == 0 {
		msg.FromSid = b.localSid
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	subject := fmt.Sprintf("%s.%s", b.prefix, msg.Topic)
	return b.client.Publish(subject, data)
}

func (b *natsSyncBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
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
