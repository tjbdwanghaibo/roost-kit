package activity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// SweepInterval is how often the activity process back-stops aggregations
// whose grace window has closed, and retries dispatches that failed.
//
// This cadence is what makes the grace window real. The window is the only
// thing that finishes an activity when a game server never notifies — and this
// service deliberately never advances an activity from its own CLOCK, only
// from a game's notification plus an expired window it already started. So an
// AdvanceExpired nobody calls means an activity that one silent game server
// leaves collecting forever, with every other game blocked on the result.
const SweepInterval = 20 * time.Second

// SweepBatch bounds one sweep of a group's expired activities, and
// DispatchBatch bounds how many due dispatches are retried per activity.
// A backlog is worked through in bounded steps rather than one unbounded pass.
const (
	SweepBatch    = 64
	DispatchBatch = 64
)

// run advances expired activities and retries due dispatches until the process
// is shutting down.
//
// It works the groups sweepGroups reports. There is no way to enumerate groups
// without an unbounded scan of the keyspace, and putting such a scan on a
// timer is the shape this repository removes — so a deployment supplies them,
// which it can, because a group is a deployment fact.
//
// The two halves are ordered deliberately: advancing an activity is what
// CREATES its dispatches, so advancing first means a result becomes deliverable
// in the same tick it was aggregated rather than the next one.
//
// A failure is logged and left for the next tick rather than returned:
// returning would take the process down, turning one stuck activity into an
// outage for every other. ErrDispatchNotDue is expected and not an error — the
// backoff has not elapsed — and ErrDispatchExhausted is logged at error level
// because it is terminal: a game server will not receive its result from any
// retry, and only a human can decide what that means for the activity.
func (s *Server) run(ctx context.Context) error {
	ticker := time.NewTicker(SweepInterval)
	defer ticker.Stop()
	service, ok := s.Service().(*Service)
	if !ok {
		// The Server only starts on the local implementation, so this cannot
		// happen — and if it ever does, sweeping nothing silently means every
		// grace window stops being enforced with nothing failing.
		return fmt.Errorf("activity server: the local capability is not a *Service, so no grace window is being enforced")
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for _, groupID := range s.sweepGroups() {
				s.sweepGroup(ctx, service, groupID)
			}
		}
	}
}

// sweepGroup advances one group's expired activities and then retries the
// dispatches they produced.
func (s *Server) sweepGroup(ctx context.Context, service *Service, groupID string) {
	advanced, err := service.AdvanceExpired(ctx, groupID, SweepBatch)
	if err != nil {
		slog.Error("activity server: advancing expired activities failed",
			"group_id", groupID, "err", err)
		return
	}
	for _, activity := range advanced {
		slog.Info("activity server: aggregation finished by grace window",
			"group_id", groupID, "activity_id", activity.Key.ActivityID,
			"phase", activity.Key.Phase, "status", activity.Status)
		due, err := service.DueDispatches(ctx, activity.Key, DispatchBatch)
		if err != nil {
			slog.Error("activity server: reading due dispatches failed",
				"activity_id", activity.Key.ActivityID, "err", err)
			continue
		}
		for _, dispatch := range due {
			_, err := service.AttemptDispatch(ctx, activity.Key, dispatch.GameSID)
			switch {
			case errors.Is(err, ErrDispatchNotDue):
				// The backoff has not elapsed. Not an error.
			case errors.Is(err, ErrDispatchExhausted):
				slog.Error("activity server: dispatch attempts exhausted; a game server will "+
					"not receive this result", "activity_id", activity.Key.ActivityID,
					"game_sid", dispatch.GameSID)
			case err != nil:
				slog.Error("activity server: dispatch attempt failed",
					"activity_id", activity.Key.ActivityID,
					"game_sid", dispatch.GameSID, "err", err)
			}
		}
	}
}

// sweepGroups is the set of groups this process sweeps.
//
// Empty by default, deliberately: enumerating groups would be an unbounded
// scan, and a deployment knows its own groups.
func (s *Server) sweepGroups() []string { return nil }
