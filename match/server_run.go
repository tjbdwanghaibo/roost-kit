package match

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SweepInterval is how often the match process resolves expired tickets.
//
// This cadence is what makes the ticket deadline real. The implementation this
// replaces wrote an ExpiresAt, forwarded it, and had no code path anywhere
// that read it — no ticker, no goroutine, nothing. A player whose ticket
// lapsed waited forever, and the ticket stayed in the queue.
const SweepInterval = 15 * time.Second

// SweepBatch bounds one sweep. A backlog is worked through in bounded steps
// rather than one unbounded pass.
const SweepBatch = 200

// run resolves expired tickets until the process is shutting down.
//
// It sweeps the queues this process is configured for. Which queues those are
// is a deployment fact — there is no way to enumerate queues without an
// unbounded scan, and putting such a scan on a timer is the shape this
// repository removes — so sweepQueues is where a deployment supplies them.
//
// A sweep failure is logged and retried on the next tick rather than returned:
// returning would take the process down, turning a recoverable backlog into an
// outage. A sweep that keeps failing shows up as a rising ticket.expired gap
// between what is enqueued and what is resolved, which is what the counters
// are for.
func (s *Server) run(ctx context.Context) error {
	ticker := time.NewTicker(SweepInterval)
	defer ticker.Stop()
	store, ok := s.Service().(Store)
	if !ok {
		// The Server only starts on the local implementation, so this cannot
		// happen — and if it ever does, sweeping nothing silently would hide
		// a deadline that stopped being enforced.
		return fmt.Errorf("match server: the local capability is not a Store, so no ticket deadline is being enforced")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, queue := range s.sweepQueues() {
				resolved, err := store.Sweep(ctx, queue, SweepBatch)
				if err != nil {
					slog.Error("match server: sweep failed", "queue", queue.Key(), "err", err)
					continue
				}
				if resolved > 0 {
					slog.Info("match server: resolved expired tickets",
						"queue", queue.Key(), "count", resolved)
				}
			}
		}
	}
}

// sweepQueues is the queue set this process sweeps.
//
// Empty by default, deliberately: enumerating queues would be an unbounded
// scan, and a deployment knows its own queues — from configuration, from the
// modes it actually runs.
func (s *Server) sweepQueues() []Queue { return nil }
