package remote_entity

import (
	"context"
	crand "crypto/rand"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"
)

var (
	ErrVersionedLockNotAcquired = errors.New("versioned lock not acquired")
	ErrVersionedLockNotOwned    = errors.New("versioned lock not owned")
	ErrVersionedLockExpired     = errors.New("versioned lock expired")
)

// versionedLock implements fredis.IVersionedLock.
type versionedLock struct {
	redis fredis.IRedis
	key   string
	token string
	ttl   time.Duration
	opts  fredis.VersionedLockOptions

	mu       sync.Mutex
	acquired bool
	version  int64

	// async touch
	touchMu     sync.Mutex
	touchActive bool
	touchCancel context.CancelFunc
	touchWg     sync.WaitGroup
}

var _ fredis.IVersionedLock = (*versionedLock)(nil)

func newVersionedLock(redis fredis.IRedis, id int64, opts fredis.VersionedLockOptions) *versionedLock {
	if opts.TTL <= 0 {
		panic("versioned lock TTL must be > 0")
	}
	key := "lock:" + opts.Key + ":" + strconv.FormatInt(id, 10)

	l := &versionedLock{
		redis: redis,
		key:   key,
		token: generateToken(),
		ttl:   opts.TTL,
		opts:  opts,
	}

	// Normalize async touch params
	if opts.AutoAsyncTouch {
		if opts.AsyncTouchExtend <= 0 {
			opts.AsyncTouchExtend = opts.TTL / 2
		}
		if opts.AsyncTouchInterval <= 0 {
			opts.AsyncTouchInterval = opts.TTL / 3
		}
		l.opts = opts
	}

	return l
}

func (l *versionedLock) TryLock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.acquired {
		return fmt.Errorf("versioned lock already acquired")
	}

	ttlMs := l.ttl.Milliseconds()
	result, err := l.redis.Eval(ctx, versionedTryLockLua, []string{l.key}, l.token, ttlMs)
	if err != nil {
		return fmt.Errorf("versioned lock redis error: %w", err)
	}

	vals, err := toInt64Slice(result)
	if err != nil {
		return fmt.Errorf("versioned lock parse error: %w", err)
	}
	if len(vals) < 2 || vals[0] == 0 {
		return ErrVersionedLockNotAcquired
	}

	l.acquired = true
	l.version = vals[1]

	if l.opts.AutoAsyncTouch {
		l.startAsyncTouchLocked()
	}

	return nil
}

func (l *versionedLock) Lock(ctx context.Context) error {
	retryCount := l.opts.RetryCount
	retryInterval := l.opts.RetryInterval

	var lastErr error
	for i := 0; i <= retryCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := l.TryLock(ctx)
		if err == nil {
			return nil
		}
		lastErr = err

		if !errors.Is(err, ErrVersionedLockNotAcquired) {
			return err // non-retryable error
		}

		if i < retryCount && retryInterval > 0 {
			backoff := retryInterval * time.Duration(1<<i)
			jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()*0.5))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jitter):
			}
		}
	}
	return lastErr
}

func (l *versionedLock) Unlock(ctx context.Context, newVersion int64, versionTTL time.Duration) error {
	return l.UnlockWithRetry(ctx, newVersion, versionTTL, l.opts.RetryCount, l.opts.RetryInterval)
}

func (l *versionedLock) UnlockWithRetry(ctx context.Context, newVersion int64, versionTTL time.Duration, retryCount int, retryInterval time.Duration) error {
	l.stopAsyncTouch()

	l.mu.Lock()
	if !l.acquired {
		l.mu.Unlock()
		return ErrVersionedLockNotOwned
	}
	l.mu.Unlock()

	if versionTTL <= 0 {
		versionTTL = l.ttl
	}
	if retryCount < 0 {
		retryCount = 0
	}

	verTTLMs := versionTTL.Milliseconds()
	var lastErr error
	for i := 0; i <= retryCount; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, err := l.redis.Eval(ctx, versionedUnlockLua, []string{l.key}, l.token, newVersion, verTTLMs)
		if err != nil {
			lastErr = fmt.Errorf("versioned lock unlock error: %w", err)
		} else {
			ok, pErr := toInt(result)
			if pErr != nil {
				lastErr = fmt.Errorf("versioned lock unlock parse: %w", pErr)
			} else if ok == 0 {
				l.mu.Lock()
				l.acquired = false
				l.mu.Unlock()
				return ErrVersionedLockNotOwned
			} else {
				l.mu.Lock()
				l.acquired = false
				l.version = newVersion
				l.mu.Unlock()
				return nil
			}
		}

		if i < retryCount && retryInterval > 0 {
			backoff := retryInterval * time.Duration(1<<i)
			jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()*0.5))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jitter):
			}
		}
	}
	return lastErr
}

func (l *versionedLock) Version() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.version
}

func (l *versionedLock) IsAcquired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquired
}

func (l *versionedLock) Touch(ctx context.Context, duration time.Duration) error {
	l.mu.Lock()
	if !l.acquired {
		l.mu.Unlock()
		return ErrVersionedLockExpired
	}
	l.mu.Unlock()

	addMs := duration.Milliseconds()
	maxTTLMs := (2 * l.ttl).Milliseconds()
	result, err := l.redis.Eval(ctx, versionedTouchLua, []string{l.key}, l.token, addMs, maxTTLMs)
	if err != nil {
		return fmt.Errorf("versioned lock touch error: %w", err)
	}

	val, err := toInt64(result)
	if err != nil {
		return fmt.Errorf("versioned lock touch parse: %w", err)
	}
	if val == -1 {
		l.mu.Lock()
		l.acquired = false
		l.mu.Unlock()
		return ErrVersionedLockExpired
	}
	return nil
}

func (l *versionedLock) Refresh(ctx context.Context) error {
	l.mu.Lock()
	if !l.acquired {
		l.mu.Unlock()
		return ErrVersionedLockExpired
	}
	l.mu.Unlock()

	ttlMs := l.ttl.Milliseconds()
	result, err := l.redis.Eval(ctx, versionedRefreshLua, []string{l.key}, l.token, ttlMs)
	if err != nil {
		return fmt.Errorf("versioned lock refresh error: %w", err)
	}
	ok, err := toInt(result)
	if err != nil {
		return fmt.Errorf("versioned lock refresh parse: %w", err)
	}
	if ok == 0 {
		l.mu.Lock()
		l.acquired = false
		l.mu.Unlock()
		return ErrVersionedLockExpired
	}
	return nil
}

func (l *versionedLock) Close() error {
	l.stopAsyncTouch()

	l.mu.Lock()
	acquired := l.acquired
	ver := l.version
	l.mu.Unlock()

	if acquired {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := l.Unlock(ctx, ver, 0)
		cancel()
		return err
	}
	return nil
}

// --- Async Touch ---

func (l *versionedLock) startAsyncTouchLocked() {
	l.touchMu.Lock()
	defer l.touchMu.Unlock()
	if l.touchActive {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.touchCancel = cancel
	l.touchActive = true
	l.touchWg.Add(1)
	go l.runAsyncTouch(ctx, l.opts.AsyncTouchExtend, l.opts.AsyncTouchInterval)
}

func (l *versionedLock) stopAsyncTouch() {
	l.touchMu.Lock()
	cancel := l.touchCancel
	active := l.touchActive
	l.touchMu.Unlock()
	if !active || cancel == nil {
		return
	}
	cancel()
	l.touchWg.Wait()
}

func (l *versionedLock) runAsyncTouch(ctx context.Context, extend, interval time.Duration) {
	defer l.touchWg.Done()
	defer func() {
		l.touchMu.Lock()
		l.touchActive = false
		l.touchCancel = nil
		l.touchMu.Unlock()
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evalCtx, cancel := context.WithTimeout(ctx, interval)
			err := l.Touch(evalCtx, extend)
			cancel()
			if err != nil {
				if errors.Is(err, ErrVersionedLockExpired) || !l.IsAcquired() {
					return
				}
			}
		}
	}
}

// --- Versioned Lock Factory ---

type versionedLockFactory struct {
	redis fredis.IRedis
}

var _ fredis.IVersionedLockFactory = (*versionedLockFactory)(nil)

func newVersionedLockFactory(redis fredis.IRedis) *versionedLockFactory {
	return &versionedLockFactory{redis: redis}
}

func (f *versionedLockFactory) NewVersionedLock(id int64, opts fredis.VersionedLockOptions) fredis.IVersionedLock {
	return newVersionedLock(f.redis, id, opts)
}

// --- Helpers ---

func generateToken() string {
	b := make([]byte, 16)
	crand.Read(b)
	return hex.EncodeToString(b)
}

func toInt64Slice(v any) ([]int64, error) {
	switch val := v.(type) {
	case []interface{}:
		result := make([]int64, len(val))
		for i, item := range val {
			n, err := toInt64(item)
			if err != nil {
				return nil, err
			}
			result[i] = n
		}
		return result, nil
	case []int64:
		return val, nil
	default:
		return nil, fmt.Errorf("unexpected type %T for int64 slice", v)
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected type %T for int64", v)
	}
}

func toInt(v any) (int, error) {
	n, err := toInt64(v)
	return int(n), err
}
