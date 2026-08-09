package remote_entity

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

type markerEvalStub struct {
	mu     sync.Mutex
	values map[string]string
}

func newMarkerEvalStub() *markerEvalStub {
	return &markerEvalStub{values: make(map[string]string)}
}

func (s *markerEvalStub) Eval(_ context.Context, script string, _ []string, args ...any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(args) == 0 {
		return nil, errors.New("missing marker field")
	}
	field := args[0].(string)
	switch script {
	case markerGetScript:
		return s.values[field], nil
	case markerMarkFenceScript:
		if len(args) != 3 {
			return nil, errors.New("invalid mark args")
		}
		expected := args[1].(string)
		owner := args[2].(string)
		if s.values[field] != expected {
			return "", nil
		}
		_, lease, err := parseMarkerLease(expected)
		if expected == "" {
			lease = entity.RemoteEntityMarkerLease{}
			err = nil
		}
		if err != nil || lease.Shared {
			return "", nil
		}
		value := formatMarkerLease(entity.RemoteEntityMarkerLease{OwnerSid: mustParseMarkerOwner(owner), Fence: lease.Fence + 1, Shared: true})
		s.values[field] = value
		return value, nil
	default:
		return nil, errors.New("unexpected script")
	}
}

func mustParseMarkerOwner(raw string) int32 {
	_, lease, _ := parseMarkerLease("local:" + raw + ":1")
	return lease.OwnerSid
}

func TestMockMarkerStorePreservesOwnershipEpochAcrossUnmark(t *testing.T) {
	store := newMockMarkerStore()
	first, err := store.Mark(context.Background(), 42, 1001)
	if err != nil || !first.Shared || first.Fence != 1 {
		t.Fatalf("first mark = %#v, err=%v", first, err)
	}
	local, err := store.Unmark(context.Background(), 42, first)
	if err != nil || local.Shared || local.Fence != 2 || local.OwnerSid != 1001 {
		t.Fatalf("unmark = %#v, err=%v", local, err)
	}
	shared, observed, err := store.GetMarker(context.Background(), 42)
	if err != nil || shared || observed != local {
		t.Fatalf("local state shared=%v lease=%#v err=%v", shared, observed, err)
	}
	second, err := store.Mark(context.Background(), 42, 1001)
	if err != nil || !second.Shared || second.Fence != 3 {
		t.Fatalf("second mark = %#v, err=%v", second, err)
	}
}

func TestParseMarkerLeasePreservesModeAndFence(t *testing.T) {
	shared, lease, err := parseMarkerLease("shared:1001:9")
	if err != nil || !shared || !lease.Shared || lease.OwnerSid != 1001 || lease.Fence != 9 {
		t.Fatalf("shared parse = %v %#v %v", shared, lease, err)
	}
	shared, lease, err = parseMarkerLease("local:1001:10")
	if err != nil || shared || lease.Shared || lease.OwnerSid != 1001 || lease.Fence != 10 {
		t.Fatalf("local parse = %v %#v %v", shared, lease, err)
	}
}

func TestRedisMarkerMarkExpectedRejectsStaleLocalLease(t *testing.T) {
	redis := newMarkerEvalStub()
	store := newRedisMarkerForEval(redis, "marks")
	expected := entity.RemoteEntityMarkerLease{OwnerSid: 1001, Fence: 2}
	redis.values["42"] = formatMarkerLease(expected)

	first, err := store.MarkExpected(context.Background(), 42, expected)
	if err != nil || !first.Shared || first.Fence != 3 || first.OwnerSid != 1001 {
		t.Fatalf("first mark = %#v, err=%v", first, err)
	}
	if _, err = store.MarkExpected(context.Background(), 42, expected); err == nil {
		t.Fatal("stale expected lease must not overwrite an existing shared lease")
	}
	shared, observed, err := store.GetMarker(context.Background(), 42)
	if err != nil || !shared || observed != first {
		t.Fatalf("marker after stale mark shared=%v lease=%#v err=%v", shared, observed, err)
	}
}
