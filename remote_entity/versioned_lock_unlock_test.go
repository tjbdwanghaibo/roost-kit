package remote_entity

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	fredis "github.com/tjbdwanghaibo/cube-core/redis"
)

// unlockEvalStub emulates the versioned lock scripts on an in-memory hash and
// can drop the response of one successful unlock, modeling the at-least-once
// retry ambiguity: the server applied the unlock but the client saw an error.
type unlockEvalStub struct {
	fredis.IRedis // unimplemented methods panic if reached; the lock only Evals
	mu            sync.Mutex
	hash          map[string]string
	fence         int64
	dropNextReply bool
	unlockCalls   int
}

func newUnlockEvalStub() *unlockEvalStub {
	return &unlockEvalStub{hash: make(map[string]string)}
}

func (s *unlockEvalStub) Eval(_ context.Context, script string, _ []string, args ...any) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch script {
	case versionedTryLockLua:
		if _, held := s.hash["owner"]; held {
			return []any{int64(0), int64(0), int64(0)}, nil
		}
		s.hash["owner"] = args[0].(string)
		version := int64(0)
		if raw, ok := s.hash["version"]; ok {
			version, _ = strconv.ParseInt(raw, 10, 64)
		}
		s.fence++
		return []any{int64(1), version, s.fence}, nil
	case versionedUnlockLua:
		s.unlockCalls++
		token := args[0].(string)
		newVersion := fmt.Sprint(args[1])
		if s.hash["owner"] == token {
			s.hash["version"] = newVersion
			delete(s.hash, "owner")
			if s.dropNextReply {
				s.dropNextReply = false
				return nil, errors.New("stub: response lost after apply")
			}
			return int64(1), nil
		}
		if s.hash["version"] == newVersion {
			return int64(2), nil
		}
		return int64(0), nil
	default:
		return nil, fmt.Errorf("stub: unexpected script")
	}
}

func TestVersionedLockUnlockRetryAfterLostResponseSucceeds(t *testing.T) {
	// Regression: the first unlock landed server-side but its response was
	// lost; the retry used to observe a missing owner and report
	// ErrVersionedLockNotOwned even though the caller's unlock had succeeded.
	// The script now recognizes its own version write and reports success.
	stub := newUnlockEvalStub()
	lock := newVersionedLock(stub, 42, fredis.VersionedLockOptions{TTL: time.Second})
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	stub.dropNextReply = true
	if err := lock.UnlockWithRetry(context.Background(), 7, time.Second, 3, time.Millisecond); err != nil {
		t.Fatalf("retried unlock after lost response must succeed: %v", err)
	}
	if stub.unlockCalls < 2 {
		t.Fatalf("scenario did not exercise the retry: calls=%d", stub.unlockCalls)
	}
	stub.mu.Lock()
	version, owned := stub.hash["version"], stub.hash["owner"]
	stub.mu.Unlock()
	if version != "7" || owned != "" {
		t.Fatalf("server state: version=%q owner=%q", version, owned)
	}

	// A genuinely foreign lock still reports NotOwned: reacquire under a new
	// token, then retry the old unlock with a stale version.
	if err := lock.TryLock(context.Background()); err != nil {
		t.Fatalf("reacquire: %v", err)
	}
	stale := newVersionedLock(stub, 42, fredis.VersionedLockOptions{TTL: time.Second})
	stale.mu.Lock()
	stale.acquired = true
	stale.token = "someone-else"
	stale.mu.Unlock()
	if err := stale.UnlockWithRetry(context.Background(), 3, time.Second, 0, 0); !errors.Is(err, ErrVersionedLockNotOwned) {
		t.Fatalf("foreign unlock err=%v, want %v", err, ErrVersionedLockNotOwned)
	}
}
