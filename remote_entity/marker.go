package remote_entity

import (
	"context"
	"fmt"
	"github.com/tjbdwanghaibo/cube-core/entity"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"strconv"
	"strings"
)

const (
	markerMarkFenceScript = `
local current = redis.call("HGET", KEYS[1], ARGV[1])
local expected = ARGV[2]
if not current then current = "" end
if current ~= expected then return "" end
local fence = 0
if current ~= "" then
  local mode, owner, currentFence = string.match(current, "^(%a+):(-?%d+):(%d+)$")
  if not mode then return "" end
  if mode ~= "local" or tonumber(owner) ~= tonumber(ARGV[3]) then return "" end
  fence = tonumber(currentFence) or 0
end
fence = fence + 1
local value = "shared:" .. ARGV[3] .. ":" .. fence
redis.call("HSET", KEYS[1], ARGV[1], value)
return value
`
	markerUnmarkFenceScript = `
local current = redis.call("HGET", KEYS[1], ARGV[1])
if current == ARGV[2] then
  local _, owner, currentFence = string.match(current, "^(%a+):(-?%d+):(%d+)$")
  local fence = (tonumber(currentFence) or 0) + 1
  local value = "local:" .. owner .. ":" .. fence
  redis.call("HSET", KEYS[1], ARGV[1], value)
  return value
end
return ""
`
	markerGetScript = `
local current = redis.call("HGET", KEYS[1], ARGV[1])
if not current then return "" end
return current
`
)

const markerRedisKey = "remote_entity:marks"

type markerEval interface {
	Eval(ctx context.Context, script string, keys []string, args ...any) (any, error)
}

type expectedMarkerStore interface {
	MarkExpected(ctx context.Context, id int64, expected entity.RemoteEntityMarkerLease) (entity.RemoteEntityMarkerLease, error)
}

// redisMarker implements entity.IRemoteEntityMarkerStore using Redis Hash.
type redisMarker struct {
	redis markerEval
	key   string
}

var _ entity.IRemoteEntityMarkerStore = (*redisMarker)(nil)

func newRedisMarker(redis fredis.IRedis, key string) *redisMarker {
	return newRedisMarkerForEval(redis, key)
}

func newRedisMarkerForEval(redis markerEval, key string) *redisMarker {
	if key == "" {
		key = markerRedisKey
	}
	return &redisMarker{redis: redis, key: key}
}

func (m *redisMarker) GetMarker(ctx context.Context, id int64) (bool, entity.RemoteEntityMarkerLease, error) {
	field := strconv.FormatInt(id, 10)
	raw, err := m.redis.Eval(ctx, markerGetScript, []string{m.key}, field)
	if err != nil {
		return false, entity.RemoteEntityMarkerLease{}, err
	}
	value := fmt.Sprint(raw)
	if value == "" {
		return false, entity.RemoteEntityMarkerLease{}, nil
	}
	shared, lease, err := parseMarkerLease(value)
	if err != nil {
		return false, entity.RemoteEntityMarkerLease{}, err
	}
	return shared, lease, nil
}

// Mark stores mark for entity.
func (m *redisMarker) Mark(ctx context.Context, id int64, ownerSid int32) (entity.RemoteEntityMarkerLease, error) {
	shared, expected, err := m.GetMarker(ctx, id)
	if err != nil {
		return entity.RemoteEntityMarkerLease{}, err
	}
	if shared {
		return entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: entity %d already marked", id)
	}
	if expected.OwnerSid != 0 && expected.OwnerSid != ownerSid {
		return entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: marker owner mismatch for %d", id)
	}
	expected.OwnerSid = ownerSid
	return m.MarkExpected(ctx, id, expected)
}

func (m *redisMarker) MarkExpected(ctx context.Context, id int64, expected entity.RemoteEntityMarkerLease) (entity.RemoteEntityMarkerLease, error) {
	if expected.Shared {
		return entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: expected local lease for %d", id)
	}
	field := strconv.FormatInt(id, 10)
	expectedValue := ""
	if expected.Fence != 0 {
		expectedValue = formatMarkerLease(expected)
	}
	raw, err := m.redis.Eval(ctx, markerMarkFenceScript, []string{m.key}, field, expectedValue, strconv.FormatInt(int64(expected.OwnerSid), 10))
	if err != nil {
		return entity.RemoteEntityMarkerLease{}, err
	}
	if fmt.Sprint(raw) == "" {
		return entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: marker compare-and-swap failed for %d", id)
	}
	_, lease, err := parseMarkerLease(fmt.Sprint(raw))
	if err != nil {
		return entity.RemoteEntityMarkerLease{}, err
	}
	return lease, nil
}

// Unmark records a local ownership state with a new fence. The hash entry is
// intentionally retained so a later Mark cannot reuse an older generation.
func (m *redisMarker) Unmark(ctx context.Context, id int64, lease entity.RemoteEntityMarkerLease) (entity.RemoteEntityMarkerLease, error) {
	if !lease.Shared || lease.Fence == 0 {
		return entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: invalid shared lease for %d", id)
	}
	field := strconv.FormatInt(id, 10)
	expected := formatMarkerLease(lease)
	raw, err := m.redis.Eval(ctx, markerUnmarkFenceScript, []string{m.key}, field, expected)
	if err != nil {
		return entity.RemoteEntityMarkerLease{}, err
	}
	shared, next, err := parseMarkerLease(fmt.Sprint(raw))
	if err != nil {
		return entity.RemoteEntityMarkerLease{}, err
	}
	if shared || next.Fence <= lease.Fence {
		return entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: marker fence mismatch for %d", id)
	}
	return next, nil
}

func parseMarkerLease(raw string) (bool, entity.RemoteEntityMarkerLease, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 3 || (parts[0] != "shared" && parts[0] != "local") {
		return false, entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: invalid marker lease %q", raw)
	}
	owner, ownerErr := strconv.ParseInt(parts[1], 10, 32)
	fence, fenceErr := strconv.ParseUint(parts[2], 10, 64)
	if ownerErr != nil || fenceErr != nil || fence == 0 {
		return false, entity.RemoteEntityMarkerLease{}, fmt.Errorf("remote_entity: invalid marker lease %q", raw)
	}
	shared := parts[0] == "shared"
	return shared, entity.RemoteEntityMarkerLease{OwnerSid: int32(owner), Fence: fence, Shared: shared}, nil
}

func formatMarkerLease(lease entity.RemoteEntityMarkerLease) string {
	mode := "local"
	if lease.Shared {
		mode = "shared"
	}
	return fmt.Sprintf("%s:%d:%d", mode, lease.OwnerSid, lease.Fence)
}
