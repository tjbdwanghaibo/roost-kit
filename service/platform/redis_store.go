package platform

import (
	"fmt"
	"strings"

	"github.com/tjbdwanghaibo/roost-core/versionstore"
)

// NewRedisOrders builds the order store over Redis.
//
// There is no storage logic here: an order is versioned state, and kit's
// versionstore.RedisStore already implements that contract — including the
// insert-only Create that turns a second callback into a replay instead of a
// second grant.
//
// It takes NO ttl, and refuses to offer one. An order is the record of a
// payment: the answer to "did this player receive what they paid for". A key
// ttl on versioned state also takes the version with the value, so an order
// that expired and was written again restarts at version 1 — meaning a
// replayed callback for an aged-out order would be treated as a first
// arrival and deliver the goods again. Retention for paid orders belongs to
// an archival job that can be audited, not to a key expiry nobody sees.
func NewRedisOrders(client versionstore.RedisClient, prefix string) (OrderStore, error) {
	if strings.TrimSpace(prefix) == "" {
		return nil, fmt.Errorf("platform: redis key prefix is required")
	}
	return versionstore.NewRedisStore(client, versionstore.RedisConfig[string, Order]{
		Prefix: prefix + ":order:",
		KeyOf:  func(orderID string) string { return orderID },
		Codec:  versionstore.JSONCodec[Order]{},
	})
}
