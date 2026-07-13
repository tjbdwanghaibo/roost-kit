package remote_entity

import "time"

// Config holds configuration for the remote entity module.
type Config struct {
	// Versioned lock settings
	LockKey    string        // versioned lock key prefix, default "e"
	LockTTL    time.Duration // lock TTL, default 24h
	RetryCount int           // lock acquire retry count, default 5
	RetryDelay time.Duration // lock acquire retry interval, default 100ms

	// Unlock settings
	UnlockRetryCount    int           // unlock retry count, default 5
	UnlockRetryInterval time.Duration // unlock retry interval, default 100ms
	VersionTTL          time.Duration // version field TTL after unlock, default 24h

	// Lock timeout for operations (context timeout)
	OpTimeout time.Duration // default 30s

	// Remote sync retry settings
	SyncRetryQueueCap    int           // default 4096
	SyncRetryInterval    time.Duration // default 500ms
	SyncRetryMaxAttempts int           // default 0 = retry forever
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		LockKey:              "e",
		LockTTL:              24 * time.Hour,
		RetryCount:           5,
		RetryDelay:           100 * time.Millisecond,
		UnlockRetryCount:     5,
		UnlockRetryInterval:  100 * time.Millisecond,
		VersionTTL:           24 * time.Hour,
		OpTimeout:            30 * time.Second,
		SyncRetryQueueCap:    4096,
		SyncRetryInterval:    500 * time.Millisecond,
		SyncRetryMaxAttempts: 0,
	}
}
