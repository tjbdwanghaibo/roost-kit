package global

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/tjbdwanghaibo/roost-core/errcode"
	"github.com/tjbdwanghaibo/roost-kit/versionstore"
)

// The segment this package is allocated, and how much of it is paired.
//
// Both are literals, and both are needed. Contiguity from the first code
// catches a hole in the middle; the count catches a truncation at the end,
// which contiguity cannot see because a shorter run is still contiguous.
// Together they pin the set exactly, so adding a public error code means
// editing this — which is right: a new code in a published segment is a
// deliberate act.
const (
	segmentFirst     = 570101
	segmentLast      = 570199
	segmentAllocated = 23
)

// expectedCodes is this package's allocated set, written out.
//
// It is a list rather than a contiguous range because 570111 is a HOLE, and
// the hole is deliberate: a catch-all "store failed" code used to sit there,
// between the routing codes and the activity codes, and it has been removed
// because nothing could produce it deliberately and an unclassified error is
// honestly CodeInternal.
//
// The activity codes are NOT renumbered down into the gap. They were
// observable in a published release — ActivityCode's hand-written switch
// really did return 570112 and up — so changing their values would break a
// client that matched on them. A permanent hole is the smaller cost, and it is
// cheaper to explain than a silently shifted code.
var expectedCodes = []int32{
	570101, 570102, 570103, 570104, 570105, 570106, 570107, 570108, 570109, 570110,
	// 570111 is the removed catch-all. Deliberately not reused: a code that
	// once meant "store failed" should not come back meaning something else.
	570112, 570113, 570114, 570115, 570116, 570117,
	570118, 570119, 570120, 570121, 570122, 570123, 570124,
}

// internalReason is what errcode.ClientError returns for anything it cannot
// classify. A sentinel that produces it has a code in name only.
const internalReason = "server error"

// codedSentinels is the pairing, as data.
var codedSentinels = map[int32]error{
	CodeRouteInvalid:       ErrRouteInvalid,
	CodeRouteMissing:       ErrRouteMissing,
	CodeRouteStale:         ErrRouteStale,
	CodeRouteMigrating:     ErrRouteMigrating,
	CodeLeaseInvalid:       ErrLeaseInvalid,
	CodeLeaseMissing:       ErrLeaseMissing,
	CodeLeaseNotHolder:     ErrLeaseNotHolder,
	CodeLeaseExpired:       ErrLeaseExpired,
	CodeRangeInvalid:       ErrRangeInvalid,
	CodeConflict:           ErrConflict,
	CodeActivityInvalid:    ErrActivityInvalid,
	CodeActivityMissing:    ErrActivityMissing,
	CodeActivityExists:     ErrActivityExists,
	CodeActivityStatus:     ErrActivityStatus,
	CodeNotifyUnexpected:   ErrNotifyUnexpected,
	CodeNotifyLate:         ErrNotifyLate,
	CodeActivityBacklog:    ErrActivityBacklog,
	CodeParticipantInvalid: ErrParticipantInvalid,
	CodeRequestInvalid:     ErrRequestInvalid,
	CodeDispatchMissing:    ErrDispatchMissing,
	CodeDispatchToken:      ErrDispatchToken,
	CodeDispatchExhausted:  ErrDispatchExhausted,
	CodeDispatchNotDue:     ErrDispatchNotDue,
}

// Every sentinel this package returns must carry its own code.
//
// The confirmed defect this answers: a whole service had no error codes, so
// "board id is empty" — a pure client mistake — reached the client as
// CodeInternal / "server error". A code constant declared beside a sentinel
// that does not carry it looks like coverage and provides none.
func TestEverySentinelCarriesItsOwnCode(t *testing.T) {
	if len(codedSentinels) == 0 {
		t.Fatal("the pairing table is empty")
	}
	for code, sentinel := range codedSentinels {
		got, reason := Error(sentinel)
		if got != code {
			t.Fatalf("sentinel %v reports code %d, want %d", sentinel, got, code)
		}
		if got == errcode.CodeInternal {
			t.Fatalf("sentinel %v reports CodeInternal; a client mistake must not read as a server fault", sentinel)
		}
		// Not merely non-empty: errcode falls back name -> message -> "server
		// error", so a sentinel defined with no text still produces a reason,
		// and that reason tells a client nothing.
		if reason == internalReason {
			t.Fatalf("sentinel %v reports the generic reason %q", sentinel, reason)
		}
	}
}

// The paired codes are exactly the allocated set.
//
// An explicit list rather than a contiguity check, because this segment has a
// documented hole at 570111 — see expectedCodes. The list is strictly stronger
// than contiguity anyway: it catches an addition, a removal and a renumbering,
// where contiguity alone misses a truncation at the end.
func TestTheCodeSegmentIsExactlyAsAllocated(t *testing.T) {
	codes := make([]int32, 0, len(codedSentinels))
	for code := range codedSentinels {
		codes = append(codes, code)
		if code < segmentFirst || code > segmentLast {
			t.Fatalf("code %d is outside this package's segment %d-%d", code, segmentFirst, segmentLast)
		}
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	if len(codes) != segmentAllocated {
		t.Fatalf("%d codes are paired, want %d", len(codes), segmentAllocated)
	}
	if len(codes) != len(expectedCodes) {
		t.Fatalf("%d codes are paired but %d are declared in expectedCodes", len(codes), len(expectedCodes))
	}
	for index, code := range codes {
		if code != expectedCodes[index] {
			t.Fatalf("at position %d the paired code is %d, expectedCodes says %d; a code was "+
				"added, removed or renumbered", index, code, expectedCodes[index])
		}
	}
	// And 570111 stays vacant: it once meant "store failed", and a code that
	// changes meaning is worse than a gap.
	for _, code := range codes {
		if code == 570111 {
			t.Fatal("570111 was reused; it previously meant \"store failed\" and a code that " +
				"changes meaning silently breaks any client that matched on it")
		}
	}
}

// The code survives however deeply the error is wrapped, which is the whole
// reason the sentinels carry it rather than a lookup table doing so: every
// existing call site wraps with fmt.Errorf and none of them had to change.
func TestTheCodeSurvivesWrapping(t *testing.T) {
	first, second := twoDistinctSentinels(t)
	firstCode, _ := Error(first)

	wrapped := fmt.Errorf("%w: context", first)
	deeper := fmt.Errorf("caller: %w", wrapped)
	joined := errors.Join(deeper, errors.New("and something else"))
	for name, err := range map[string]error{"once": wrapped, "twice": deeper, "joined": joined} {
		if code := Code(err); code != firstCode {
			t.Fatalf("%s-wrapped error reports code %d, want %d", name, code, firstCode)
		}
	}
	// errors.Is still discriminates, so no existing test or caller changed
	// meaning.
	if !errors.Is(wrapped, first) {
		t.Fatal("errors.Is no longer matches the sentinel it was wrapped with")
	}
	if errors.Is(wrapped, second) {
		t.Fatal("errors.Is matches a different sentinel; the codes are not discriminating")
	}
	// When two coded errors are wrapped together, the FIRST wins. That is what
	// makes a refusal wrapping a caller's own reason report the refusal.
	if code := Code(fmt.Errorf("%w: %w", first, second)); code != firstCode {
		t.Fatalf("a doubly-wrapped error reports code %d, want the outer %d", code, firstCode)
	}
}

// An unclassified failure reads as CodeInternal — the honest answer, and the
// reason there is no service-specific catch-all code.
func TestAnUnclassifiedFailureReadsAsInternal(t *testing.T) {
	if code := Code(errors.New("dial redis: connection refused")); code != errcode.CodeInternal {
		t.Fatalf("an unclassified failure reports code %d, want CodeInternal", code)
	}
	if code, reason := Error(nil); code != CodeOK || reason != "" {
		t.Fatalf("a nil error reports (%d, %q), want (CodeOK, \"\")", code, reason)
	}
}

// A foreign sentinel this package maps deliberately must NOT fall through to
// the internal code. Compare-and-set exhaustion under contention is a real, retryable outcome.
func TestAForeignSentinelIsMappedIntoThisSegment(t *testing.T) {
	err := fmt.Errorf("update: %w", versionstore.ErrConflict)
	code := Code(err)
	if code == errcode.CodeInternal {
		t.Fatalf("%v reached the caller as CodeInternal; a foreign sentinel needs a deliberate mapping", err)
	}
	if code != CodeConflict {
		t.Fatalf("%v maps to %d, want %d", err, code, CodeConflict)
	}
}

// twoDistinctSentinels returns the lowest two coded sentinels, so the wrapping
// test does not depend on any particular error existing.
func twoDistinctSentinels(t *testing.T) (error, error) {
	t.Helper()
	codes := make([]int, 0, len(codedSentinels))
	for code := range codedSentinels {
		codes = append(codes, int(code))
	}
	sort.Ints(codes)
	if len(codes) < 2 {
		t.Skip("this package has fewer than two coded sentinels")
	}
	return codedSentinels[int32(codes[0])], codedSentinels[int32(codes[1])]
}
