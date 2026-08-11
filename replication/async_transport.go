package replication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/tjbdwanghaibo/cube-core/replication"
)

type Channel uint8

const (
	ChannelDatagram Channel = iota + 1
	ChannelReliable
)

type SendError struct {
	Session core.SessionID
	Channel Channel
	Err     error
}

func (sendError SendError) Error() string {
	if sendError.Err == nil {
		return "replication transport: unknown send error"
	}
	return sendError.Err.Error()
}
func (sendError SendError) Unwrap() error { return sendError.Err }

// ErrorHandler must return promptly. Panics are contained and counted, but a
// blocking handler still blocks the affected session lane.
type ErrorHandler func(SendError)

type AsyncTransportConfig struct {
	MaxSessions          int
	ReliableQueueSize    int
	MaxDatagramsPerFrame int
	MaxDatagramBytes     int
	MaxReliableBytes     int
	SendTimeout          time.Duration
	// AllowOpaqueDatagrams disables replication header, checksum, and complete
	// frame-batch validation. Keep false unless a trusted upstream has already
	// performed equivalent validation.
	AllowOpaqueDatagrams bool
	OnError              ErrorHandler
}

func DefaultAsyncTransportConfig() AsyncTransportConfig {
	return AsyncTransportConfig{
		MaxSessions:          4096,
		ReliableQueueSize:    256,
		MaxDatagramsPerFrame: core.DefaultLimits().MaxFragments,
		MaxDatagramBytes:     core.DefaultMaxDatagram,
		MaxReliableBytes:     1 << 20,
		SendTimeout:          250 * time.Millisecond,
	}
}

type AsyncTransport struct {
	mu         sync.RWMutex
	downstream core.Transport
	config     AsyncTransportConfig
	ctx        context.Context
	cancel     context.CancelFunc
	sessions   map[core.SessionID]*sessionQueue
	closed     bool
	wait       sync.WaitGroup
	closeOnce  sync.Once
	closeDone  chan struct{}
	stats      asyncCounters
}

type sessionQueue struct {
	id     core.SessionID
	owner  *AsyncTransport
	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	closing      bool
	failure      error
	latest       [][]byte
	reliable     [][]byte
	datagramBusy bool
	reliableBusy bool
	latestWake   chan struct{}
	reliableWake chan struct{}
	workersDone  atomic.Uint32
}

type asyncCounters struct {
	datagramFramesQueued  atomic.Uint64
	datagramFramesSent    atomic.Uint64
	datagramFramesDropped atomic.Uint64
	datagramBytesSent     atomic.Uint64
	reliableQueued        atomic.Uint64
	reliableSent          atomic.Uint64
	reliableBytesSent     atomic.Uint64
	reliableBackpressure  atomic.Uint64
	reliableAbandoned     atomic.Uint64
	sendErrors            atomic.Uint64
	handlerPanics         atomic.Uint64
}

type AsyncTransportStats struct {
	ActiveSessions        int
	DrainingSessions      int
	PendingDatagramFrames int
	PendingReliable       int
	DatagramSendsInFlight int
	ReliableSendsInFlight int
	DatagramFramesQueued  uint64
	DatagramFramesSent    uint64
	DatagramFramesDropped uint64
	DatagramBytesSent     uint64
	ReliableQueued        uint64
	ReliableSent          uint64
	ReliableBytesSent     uint64
	ReliableBackpressure  uint64
	ReliableAbandoned     uint64
	SendErrors            uint64
	ErrorHandlerPanics    uint64
}

func NewAsyncTransport(downstream core.Transport, config AsyncTransportConfig) (*AsyncTransport, error) {
	if isNilInterface(downstream) {
		return nil, ErrTransportRequired
	}
	defaults := DefaultAsyncTransportConfig()
	if config.MaxSessions <= 0 {
		config.MaxSessions = defaults.MaxSessions
	}
	if config.ReliableQueueSize <= 0 {
		config.ReliableQueueSize = defaults.ReliableQueueSize
	}
	if config.MaxDatagramsPerFrame <= 0 {
		config.MaxDatagramsPerFrame = defaults.MaxDatagramsPerFrame
	}
	if config.MaxDatagramBytes <= 0 {
		config.MaxDatagramBytes = defaults.MaxDatagramBytes
	}
	if config.MaxReliableBytes <= 0 {
		config.MaxReliableBytes = defaults.MaxReliableBytes
	}
	if config.SendTimeout <= 0 {
		config.SendTimeout = defaults.SendTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &AsyncTransport{
		downstream: downstream, config: config, ctx: ctx, cancel: cancel,
		sessions: make(map[core.SessionID]*sessionQueue), closeDone: make(chan struct{}),
	}, nil
}

// RegisterSession starts two independent workers: latest-only datagrams and
// bounded reliable messages. This avoids reliable stream stalls blocking state.
func (transport *AsyncTransport) RegisterSession(info core.SessionInfo) error {
	id := info.ID
	if transport == nil || id == 0 {
		return ErrSessionNotRegistered
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.closed {
		return ErrTransportClosed
	}
	if _, exists := transport.sessions[id]; exists {
		return ErrSessionAlreadyExists
	}
	if len(transport.sessions) >= transport.config.MaxSessions {
		return ErrSessionLimit
	}
	if lifecycle, ok := transport.downstream.(core.SessionTransport); ok {
		if err := lifecycle.RegisterSession(info); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(transport.ctx)
	queue := &sessionQueue{
		id: id, owner: transport, ctx: ctx, cancel: cancel,
		latestWake: make(chan struct{}, 1), reliableWake: make(chan struct{}, 1),
	}
	transport.sessions[id] = queue
	transport.wait.Add(2)
	go queue.runDatagrams()
	go queue.runReliable()
	return nil
}

// RemoveSession cancels queued work immediately. Room shutdown should use
// Close when reliable messages need a bounded graceful drain.
func (transport *AsyncTransport) RemoveSession(id core.SessionID) bool {
	if transport == nil || id == 0 {
		return false
	}
	transport.mu.Lock()
	queue, exists := transport.sessions[id]
	transport.mu.Unlock()
	if exists {
		queue.cancelNow()
	}
	return exists
}

func (transport *AsyncTransport) SendDatagram(ctx context.Context, id core.SessionID, payload []byte) error {
	return transport.SendDatagramBatch(ctx, id, [][]byte{payload})
}

// SendDatagramBatch atomically replaces the pending state frame. It copies all
// packets before admission, so callers may safely reuse their buffers.
func (transport *AsyncTransport) SendDatagramBatch(ctx context.Context, id core.SessionID, packets [][]byte) error {
	if transport == nil {
		return ErrTransportClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(packets) == 0 || len(packets) > transport.config.MaxDatagramsPerFrame {
		return ErrInvalidDatagramBatch
	}
	if !transport.config.AllowOpaqueDatagrams {
		if err := validateDatagramBatch(packets, transport.config); err != nil {
			return err
		}
	}
	queue, err := transport.session(id)
	if err != nil {
		return err
	}
	batch := make([][]byte, len(packets))
	for index, packet := range packets {
		if len(packet) == 0 || len(packet) > transport.config.MaxDatagramBytes {
			return ErrInvalidDatagramBatch
		}
		batch[index] = append([]byte(nil), packet...)
	}
	queue.mu.Lock()
	if err := queue.admissionErrorLocked(); err != nil {
		queue.mu.Unlock()
		return err
	}
	if queue.latest != nil {
		transport.stats.datagramFramesDropped.Add(1)
	}
	queue.latest = batch
	queue.mu.Unlock()
	transport.stats.datagramFramesQueued.Add(1)
	signal(queue.latestWake)
	return nil
}

func (transport *AsyncTransport) SendReliable(ctx context.Context, id core.SessionID, payload []byte) error {
	if transport == nil {
		return ErrTransportClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(payload) == 0 || len(payload) > transport.config.MaxReliableBytes {
		return ErrReliableMessageTooBig
	}
	queue, err := transport.session(id)
	if err != nil {
		return err
	}
	message := append([]byte(nil), payload...)
	queue.mu.Lock()
	if err := queue.admissionErrorLocked(); err != nil {
		queue.mu.Unlock()
		return err
	}
	if len(queue.reliable) >= transport.config.ReliableQueueSize {
		queue.mu.Unlock()
		transport.stats.reliableBackpressure.Add(1)
		return ErrReliableBackpressure
	}
	queue.reliable = append(queue.reliable, message)
	queue.mu.Unlock()
	transport.stats.reliableQueued.Add(1)
	signal(queue.reliableWake)
	return nil
}

func (transport *AsyncTransport) session(id core.SessionID) (*sessionQueue, error) {
	if transport == nil {
		return nil, ErrTransportClosed
	}
	transport.mu.RLock()
	if transport.closed {
		transport.mu.RUnlock()
		return nil, ErrTransportClosed
	}
	queue := transport.sessions[id]
	transport.mu.RUnlock()
	if queue == nil {
		return nil, ErrSessionNotRegistered
	}
	return queue, nil
}

// Close rejects new work, drains each session's currently admitted work, and
// then stops. If ctx expires, in-flight sends are cancelled and ctx.Err is
// returned. Downstream transports must honor the provided send context.
func (transport *AsyncTransport) Close(ctx context.Context) error {
	if transport == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	transport.closeOnce.Do(func() {
		transport.mu.Lock()
		transport.closed = true
		for _, queue := range transport.sessions {
			queue.beginClose()
		}
		transport.mu.Unlock()
		go func() {
			transport.wait.Wait()
			transport.cancel()
			close(transport.closeDone)
		}()
	})
	select {
	case <-transport.closeDone:
		return nil
	default:
	}
	select {
	case <-transport.closeDone:
		return nil
	case <-ctx.Done():
		transport.cancel()
		return ctx.Err()
	}
}

func (transport *AsyncTransport) Stats() AsyncTransportStats {
	if transport == nil {
		return AsyncTransportStats{}
	}
	transport.mu.RLock()
	active, draining, pendingDatagrams, pendingReliable, datagramBusy, reliableBusy := 0, 0, 0, 0, 0, 0
	for _, queue := range transport.sessions {
		queue.mu.Lock()
		if queue.closing {
			draining++
		} else {
			active++
		}
		if queue.latest != nil {
			pendingDatagrams++
		}
		pendingReliable += len(queue.reliable)
		if queue.datagramBusy {
			datagramBusy++
		}
		if queue.reliableBusy {
			reliableBusy++
		}
		queue.mu.Unlock()
	}
	transport.mu.RUnlock()
	return AsyncTransportStats{
		ActiveSessions:        active,
		DrainingSessions:      draining,
		PendingDatagramFrames: pendingDatagrams,
		PendingReliable:       pendingReliable,
		DatagramSendsInFlight: datagramBusy,
		ReliableSendsInFlight: reliableBusy,
		DatagramFramesQueued:  transport.stats.datagramFramesQueued.Load(),
		DatagramFramesSent:    transport.stats.datagramFramesSent.Load(),
		DatagramFramesDropped: transport.stats.datagramFramesDropped.Load(),
		DatagramBytesSent:     transport.stats.datagramBytesSent.Load(),
		ReliableQueued:        transport.stats.reliableQueued.Load(),
		ReliableSent:          transport.stats.reliableSent.Load(),
		ReliableBytesSent:     transport.stats.reliableBytesSent.Load(),
		ReliableBackpressure:  transport.stats.reliableBackpressure.Load(),
		ReliableAbandoned:     transport.stats.reliableAbandoned.Load(),
		SendErrors:            transport.stats.sendErrors.Load(),
		ErrorHandlerPanics:    transport.stats.handlerPanics.Load(),
	}
}

func (queue *sessionQueue) beginClose() {
	queue.mu.Lock()
	queue.closing = true
	queue.mu.Unlock()
	signal(queue.latestWake)
	signal(queue.reliableWake)
}

func (queue *sessionQueue) cancelNow() {
	queue.mu.Lock()
	queue.closing = true
	if queue.latest != nil {
		queue.owner.stats.datagramFramesDropped.Add(1)
	}
	queue.owner.stats.reliableAbandoned.Add(uint64(len(queue.reliable)))
	queue.latest = nil
	queue.reliable = nil
	queue.mu.Unlock()
	queue.cancel()
}

func (queue *sessionQueue) runDatagrams() {
	defer queue.workerDone()
	for {
		select {
		case <-queue.ctx.Done():
			return
		case <-queue.latestWake:
		}
		for {
			batch, exit := queue.takeLatest()
			if exit {
				return
			}
			if batch == nil {
				break
			}
			queue.sendDatagrams(batch)
		}
	}
}

func (queue *sessionQueue) runReliable() {
	defer queue.workerDone()
	for {
		select {
		case <-queue.ctx.Done():
			return
		case <-queue.reliableWake:
		}
		for {
			message, exit := queue.takeReliable()
			if exit {
				return
			}
			if message == nil {
				break
			}
			if !queue.sendReliable(message) {
				return
			}
		}
	}
}

func (queue *sessionQueue) takeLatest() ([][]byte, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	batch := queue.latest
	queue.latest = nil
	if batch != nil {
		queue.datagramBusy = true
	}
	return batch, batch == nil && queue.closing
}

func (queue *sessionQueue) takeReliable() ([]byte, bool) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.reliable) == 0 {
		return nil, queue.closing
	}
	message := queue.reliable[0]
	queue.reliable[0] = nil
	queue.reliable = queue.reliable[1:]
	queue.reliableBusy = true
	return message, false
}

func (queue *sessionQueue) sendDatagrams(batch [][]byte) {
	ctx, cancel := context.WithTimeout(queue.ctx, queue.owner.config.SendTimeout)
	defer cancel()
	downstream := queue.owner.downstream
	var err error
	if batched, ok := downstream.(core.DatagramBatchTransport); ok {
		err = batched.SendDatagramBatch(ctx, queue.id, batch)
	} else {
		for _, packet := range batch {
			if err = downstream.SendDatagram(ctx, queue.id, packet); err != nil {
				break
			}
		}
	}
	if err != nil {
		queue.owner.report(SendError{Session: queue.id, Channel: ChannelDatagram, Err: err})
		queue.mu.Lock()
		queue.datagramBusy = false
		queue.mu.Unlock()
		return
	}
	queue.owner.stats.datagramFramesSent.Add(1)
	for _, packet := range batch {
		queue.owner.stats.datagramBytesSent.Add(uint64(len(packet)))
	}
	queue.mu.Lock()
	queue.datagramBusy = false
	queue.mu.Unlock()
}

func (queue *sessionQueue) sendReliable(message []byte) bool {
	ctx, cancel := context.WithTimeout(queue.ctx, queue.owner.config.SendTimeout)
	defer cancel()
	if err := queue.owner.downstream.SendReliable(ctx, queue.id, message); err != nil {
		queue.owner.report(SendError{Session: queue.id, Channel: ChannelReliable, Err: err})
		queue.fail(err)
		return false
	}
	queue.owner.stats.reliableSent.Add(1)
	queue.owner.stats.reliableBytesSent.Add(uint64(len(message)))
	queue.mu.Lock()
	queue.reliableBusy = false
	queue.mu.Unlock()
	return true
}

func (transport *AsyncTransport) report(sendError SendError) {
	transport.stats.sendErrors.Add(1)
	if transport.config.OnError != nil {
		func() {
			defer func() {
				if recover() != nil {
					transport.stats.handlerPanics.Add(1)
				}
			}()
			transport.config.OnError(sendError)
		}()
	}
}

func (queue *sessionQueue) admissionErrorLocked() error {
	if queue.failure != nil {
		return fmt.Errorf("%w: %v", ErrSessionFailed, queue.failure)
	}
	if queue.closing {
		return ErrSessionNotRegistered
	}
	return nil
}

func (queue *sessionQueue) fail(err error) {
	queue.mu.Lock()
	queue.failure = err
	queue.closing = true
	queue.reliableBusy = false
	if queue.latest != nil {
		queue.owner.stats.datagramFramesDropped.Add(1)
	}
	queue.owner.stats.reliableAbandoned.Add(uint64(len(queue.reliable)))
	queue.latest = nil
	queue.reliable = nil
	queue.mu.Unlock()
	queue.cancel()
}

func (queue *sessionQueue) workerDone() {
	if queue.workersDone.Add(1) == 2 {
		if lifecycle, ok := queue.owner.downstream.(core.SessionTransport); ok {
			lifecycle.RemoveSession(queue.id)
		}
		queue.owner.mu.Lock()
		if queue.owner.sessions[queue.id] == queue {
			delete(queue.owner.sessions, queue.id)
		}
		queue.owner.mu.Unlock()
	}
	queue.owner.wait.Done()
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func validateDatagramBatch(packets [][]byte, config AsyncTransportConfig) error {
	limits := core.DefaultLimits()
	limits.MaxDatagramBytes = config.MaxDatagramBytes
	limits.MaxFragments = config.MaxDatagramsPerFrame
	var first core.DatagramHeader
	seen := make([]bool, len(packets))
	for index, packet := range packets {
		header, err := core.InspectDatagram(packet, limits)
		if err != nil {
			return fmt.Errorf("%w: packet %d: %v", ErrInvalidDatagramBatch, index, err)
		}
		if int(header.ChunkCount) != len(packets) || int(header.ChunkIndex) >= len(seen) || seen[header.ChunkIndex] {
			return ErrInvalidDatagramBatch
		}
		seen[header.ChunkIndex] = true
		if index == 0 {
			first = header
			continue
		}
		if header.RoomID != first.RoomID || header.Epoch != first.Epoch || header.Tick != first.Tick ||
			header.BaseTick != first.BaseTick || header.Sequence != first.Sequence || header.ChunkCount != first.ChunkCount || header.Flags != first.Flags {
			return ErrInvalidDatagramBatch
		}
	}
	return nil
}

var _ core.Transport = (*AsyncTransport)(nil)
var _ core.DatagramBatchTransport = (*AsyncTransport)(nil)
var _ core.SessionTransport = (*AsyncTransport)(nil)
