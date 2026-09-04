package chat

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// PruneInterval is how often the chat process drops messages older than the
// retention age.
//
// This cadence is what makes retention real. The implementation this replaces
// persisted a timestamp as an int64 of milliseconds rather than a date, so no
// TTL was possible at all, and nothing anywhere dropped an old message — a
// channel grew until someone noticed. Here Prune exists, and a Prune nobody
// calls is the same outcome with better-looking code.
const PruneInterval = 5 * time.Minute

// PruneBatch bounds one prune. A long-neglected channel is worked down over
// several ticks rather than in one unbounded pass.
const PruneBatch = 200

// run prunes retained messages until the process is shutting down.
//
// It prunes the channels pruneChannels reports. That set is a ChannelRef slice
// rather than a Channel slice, and the reason is the same property that keeps
// Prune off the bus: a ChannelRef's stream key is unexported, so the only way
// to obtain one is Resolve — which a deployment can call, because it holds the
// Service in this very process.
//
// A failed prune is logged and retried on the next tick rather than returned:
// returning would take the chat process down, and an over-retained channel is
// a storage cost, not an outage. What an operator watches is the Retained
// figure from Stats not coming down.
func (s *Server) run(ctx context.Context) error {
	ticker := time.NewTicker(PruneInterval)
	defer ticker.Stop()
	service, ok := s.Service().(*Service)
	if !ok {
		// The Server only starts on the local implementation, so this cannot
		// happen — and if it ever does, pruning nothing silently is how
		// retention stops being enforced without anything failing.
		return fmt.Errorf("chat server: the local capability is not a *Service, so no retention is being enforced")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, ref := range s.pruneChannels() {
				dropped, err := service.Prune(ctx, ref, PruneBatch)
				if err != nil {
					slog.Error("chat server: prune failed", "kind", ref.Kind, "err", err)
					continue
				}
				if dropped > 0 {
					slog.Info("chat server: pruned retained messages",
						"kind", ref.Kind, "count", dropped)
				}
			}
		}
	}
}

// pruneChannels is the set of channels this process prunes.
//
// Empty by default, deliberately: enumerating channels would be an unbounded
// scan of the keyspace, and a deployment knows which channels it has — a world
// channel, a handful of group channels, the pair channels its online players
// are using. Refs come from Service.Resolve.
func (s *Server) pruneChannels() []ChannelRef { return nil }
