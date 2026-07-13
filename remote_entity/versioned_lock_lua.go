package remote_entity

// versionedTryLockLua: HSETNX owner → HGET version → PEXPIRE; returns {1, version} or {0, 0}
const versionedTryLockLua = `
local ok = redis.call("HSETNX", KEYS[1], "owner", ARGV[1])
if ok == 1 then
    local ver = redis.call("HGET", KEYS[1], "version")
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
    if ver == false then
        return {1, 0}
    end
    return {1, tonumber(ver)}
end
return {0, 0}
`

// versionedUnlockLua: verify owner → HSET version → HDEL owner → PEXPIRE; returns 1 or 0
const versionedUnlockLua = `
local cur = redis.call("HGET", KEYS[1], "owner")
if cur ~= ARGV[1] then
    return 0
end
redis.call("HSET", KEYS[1], "version", ARGV[2])
redis.call("HDEL", KEYS[1], "owner")
redis.call("PEXPIRE", KEYS[1], ARGV[3])
return 1
`

// versionedTouchLua: verify owner → add duration to remaining PTTL (capped at max); returns newTTL or -1
const versionedTouchLua = `
if redis.call("HGET", KEYS[1], "owner") ~= ARGV[1] then
    return -1
end
local pttl = redis.call("PTTL", KEYS[1])
if pttl < 0 then
    pttl = 0
end
local newTTL = pttl + tonumber(ARGV[2])
local maxTTL = tonumber(ARGV[3])
if maxTTL > 0 and newTTL > maxTTL then
    newTTL = maxTTL
end
redis.call("PEXPIRE", KEYS[1], newTTL)
return newTTL
`

// versionedRefreshLua: verify owner → PEXPIRE to TTL; returns 1 or 0
const versionedRefreshLua = `
if redis.call("HGET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`
