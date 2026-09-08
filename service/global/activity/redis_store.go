package activity

import (
	"fmt"
	"strings"
	"time"

	"github.com/tjbdwanghaibo/roost-core/versionstore"
)

// RedisStores are the six stores this service needs, over Redis.
//
// There is no storage logic here, and that is the point. The implementation
// this replaces had a Redis store whose Update used compare-and-set and a
// DAO-backed store whose Update read and then wrote unconditionally, both
// satisfying one interface — so which one was configured decided whether the
// documented CAS invariant held. Here there is one implementation, in kit,
// whose contract has no unconditional write.
type RedisStores struct {
	Activities   versionstore.Store[Key, Activity]
	Participants versionstore.Store[ParticipantKey, Participant]
	Ledger       versionstore.Store[RequestKey, ProgressReservation]
	Audits       versionstore.Store[Key, NotifyAuditLog]
	Dispatches   versionstore.Store[DispatchKey, Dispatch]
	Windows      versionstore.Store[string, Window]
}

// NewRedisStores builds them.
//
// The ledger is the only store with a key TTL, and it must have one: its
// entries are progress reservations, which are unbounded in number and whose
// job is finished once no client will retry. reservationTTL has to exceed the
// longest client retry horizon — past it a replay is indistinguishable from a
// new request, and the progress is applied twice.
//
// Nothing else gets a TTL, because a TTL on versioned state takes the version
// with the value: a key that expired and was written again restarts at version
// 1, and every fence a caller passes becomes a comparison against a version
// that just reset.
func NewRedisStores(client versionstore.RedisClient, prefix string, reservationTTL time.Duration) (RedisStores, error) {
	if strings.TrimSpace(prefix) == "" {
		return RedisStores{}, fmt.Errorf("activity: redis key prefix is required")
	}
	if reservationTTL <= 0 {
		return RedisStores{}, fmt.Errorf("activity: reservation ttl must be positive; it must also " +
			"exceed the longest client retry horizon, or a replay becomes a second apply")
	}
	var (
		stores RedisStores
		err    error
	)
	if stores.Activities, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[Key, Activity]{
		Prefix: prefix + ":act:", KeyOf: Key.String, Codec: versionstore.JSONCodec[Activity]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("activity: activity store: %w", err)
	}
	if stores.Participants, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[ParticipantKey, Participant]{
		Prefix: prefix + ":part:", KeyOf: ParticipantKey.String, Codec: versionstore.JSONCodec[Participant]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("activity: participant store: %w", err)
	}
	if stores.Ledger, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[RequestKey, ProgressReservation]{
		Prefix: prefix + ":req:", KeyOf: RequestKey.String, Codec: versionstore.JSONCodec[ProgressReservation]{},
		TTL: reservationTTL,
	}); err != nil {
		return RedisStores{}, fmt.Errorf("activity: progress ledger: %w", err)
	}
	if stores.Audits, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[Key, NotifyAuditLog]{
		Prefix: prefix + ":audit:", KeyOf: Key.String, Codec: versionstore.JSONCodec[NotifyAuditLog]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("activity: audit store: %w", err)
	}
	if stores.Dispatches, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[DispatchKey, Dispatch]{
		Prefix: prefix + ":disp:", KeyOf: DispatchKey.String, Codec: versionstore.JSONCodec[Dispatch]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("activity: dispatch store: %w", err)
	}
	if stores.Windows, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[string, Window]{
		Prefix: prefix + ":win:", KeyOf: func(groupID string) string { return groupID },
		Codec: versionstore.JSONCodec[Window]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("activity: window store: %w", err)
	}
	return stores, nil
}
