package global

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tjbdwanghaibo/roost-kit/versionstore"
)

// RedisStores are the stores this package needs, over Redis: two for routing
// and leases, six for activity coordination.
//
// There is no storage logic here, and for this package that is the entire
// point of the design. The implementation this replaces had FOUR hand-written
// stores per concern — a Redis variant whose Update used compare-and-set and a
// DAO variant whose Update read and then wrote unconditionally — satisfying
// one interface, so the type system could not tell them apart and whichever
// was configured decided whether the documented invariant held. Here there is
// one implementation, in kit, whose contract has no unconditional write.
type RedisStores struct {
	Routes versionstore.Store[int32, RouteBinding]
	Leases versionstore.Store[int32, GameLease]

	Activities   versionstore.Store[ActivityKey, Activity]
	Participants versionstore.Store[ParticipantKey, Participant]
	Ledger       versionstore.Store[RequestKey, ProgressReservation]
	Audits       versionstore.Store[ActivityKey, NotifyAuditLog]
	Dispatches   versionstore.Store[DispatchKey, Dispatch]
	Windows      versionstore.Store[string, ActivityWindow]
}

// NewRedisStores builds them.
//
// The ledger is the only store with a key TTL, and it must have one: its
// entries are progress reservations, which are unbounded in number and whose
// job is finished once no client will retry. reservationTTL has to exceed the
// longest client retry horizon — past it a replay is indistinguishable from a
// new request, and the progress is applied twice.
//
// Nothing else gets a TTL. A lease in particular must not: a TTL on versioned
// state takes the version with the value, so a lease key that expired and was
// written again restarts at version 1, and the incarnation fence that a
// renewal must pass becomes a comparison against a version that just reset.
// A lease's expiry is a FIELD this package reads.
func NewRedisStores(client versionstore.RedisClient, prefix string, reservationTTL time.Duration) (RedisStores, error) {
	if strings.TrimSpace(prefix) == "" {
		return RedisStores{}, fmt.Errorf("global: redis key prefix is required")
	}
	if reservationTTL <= 0 {
		return RedisStores{}, fmt.Errorf("global: reservation ttl must be positive; it must also " +
			"exceed the longest client retry horizon, or a replay becomes a second apply")
	}
	int32Key := func(id int32) string { return strconv.FormatInt(int64(id), 10) }

	var (
		stores RedisStores
		err    error
	)
	if stores.Routes, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[int32, RouteBinding]{
		Prefix: prefix + ":route:", KeyOf: int32Key, Codec: versionstore.JSONCodec[RouteBinding]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: route store: %w", err)
	}
	if stores.Leases, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[int32, GameLease]{
		Prefix: prefix + ":lease:", KeyOf: int32Key, Codec: versionstore.JSONCodec[GameLease]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: lease store: %w", err)
	}
	if stores.Activities, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[ActivityKey, Activity]{
		Prefix: prefix + ":act:", KeyOf: ActivityKey.String, Codec: versionstore.JSONCodec[Activity]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: activity store: %w", err)
	}
	if stores.Participants, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[ParticipantKey, Participant]{
		Prefix: prefix + ":part:", KeyOf: ParticipantKey.String, Codec: versionstore.JSONCodec[Participant]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: participant store: %w", err)
	}
	if stores.Ledger, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[RequestKey, ProgressReservation]{
		Prefix: prefix + ":req:", KeyOf: RequestKey.String, Codec: versionstore.JSONCodec[ProgressReservation]{},
		TTL: reservationTTL,
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: progress ledger: %w", err)
	}
	if stores.Audits, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[ActivityKey, NotifyAuditLog]{
		Prefix: prefix + ":audit:", KeyOf: ActivityKey.String, Codec: versionstore.JSONCodec[NotifyAuditLog]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: audit store: %w", err)
	}
	if stores.Dispatches, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[DispatchKey, Dispatch]{
		Prefix: prefix + ":disp:", KeyOf: DispatchKey.String, Codec: versionstore.JSONCodec[Dispatch]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: dispatch store: %w", err)
	}
	if stores.Windows, err = versionstore.NewRedisStore(client, versionstore.RedisConfig[string, ActivityWindow]{
		Prefix: prefix + ":win:", KeyOf: func(groupID string) string { return groupID },
		Codec: versionstore.JSONCodec[ActivityWindow]{},
	}); err != nil {
		return RedisStores{}, fmt.Errorf("global: window store: %w", err)
	}
	return stores, nil
}
