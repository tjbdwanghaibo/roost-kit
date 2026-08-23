package sync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	stdsync "sync"
	"sync/atomic"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
	coreentitysync "github.com/tjbdwanghaibo/cube-core/entitysync"
)

var (
	ErrRoomIDInvalid             = errors.New("sync: room id is invalid")
	ErrRoomFrameSinkRequired     = errors.New("sync: reliable room frame sink is required")
	ErrRoomSubjectInvalid        = errors.New("sync: room subject is invalid")
	ErrRoomSubjectNotRegistered  = errors.New("sync: room subject is not registered")
	ErrRoomSubjectAlreadyExists  = errors.New("sync: room subject is already registered")
	ErrRoomSubjectHasSubscribers = errors.New("sync: room subject still has subscribers")
	ErrRoomEnvelopeInvalid       = errors.New("sync: room delivery envelope is invalid")
	ErrRoomFrameAdmission        = errors.New("sync: room frame admission failed")
	ErrRoomReplicationStopped    = errors.New("sync: room replication is stopped")
	ErrRoomSubjectRetiring       = errors.New("sync: room subject is retiring")
	ErrRoomSubjectLimit          = errors.New("sync: room subject limit exceeded")
	ErrRoomSubscriberLimit       = errors.New("sync: room subscriber limit exceeded")
)

const DefaultRoomReplicationInterval = 50 * time.Millisecond

// RoomFrameEntry is one subject update in a receiver-specific room frame.
// Payload bytes are immutable and may be shared by every receiver using the
// same profile.
type RoomFrameEntry struct {
	Kind   coreentitysync.EnvelopeKind
	Update entity.SubjectSyncUpdate
}

// RoomFrame is the transport-neutral unit admitted by the room replication
// layer. Frame is monotonic within a room. SessionSequence is monotonic for a
// (room, subscriber) stream and is suitable for loss detection and replay.
type RoomFrame struct {
	RoomID          int64
	Frame           uint64
	Subscriber      coreentitysync.SubscriberRef
	SessionSequence uint64
	Entries         []RoomFrameEntry
}

// ReliableRoomFrameSink must atomically admit the complete slice. Returning
// nil transfers responsibility for buffering, persistence/history and network
// delivery to the sink. On error it must retain none of the frames.
type ReliableRoomFrameSink interface {
	AdmitRoomFrames(context.Context, []RoomFrame) error
}

// RoomSubjectLifecycle lets a transport release compact per-room object IDs
// only after the subject has no subscribers and has been unregistered.
type RoomSubjectLifecycle interface {
	ReleaseRoomSubject(roomID, subjectID int64)
}

type RoomSubscriberLifecycle interface {
	ReleaseRoomSubscriber(context.Context, int64, coreentitysync.SubscriberRef)
}

type RoomLifecycle interface {
	ResetRoom(int64)
}

type ReliableRoomFrameSinkFunc func(context.Context, []RoomFrame) error

func (f ReliableRoomFrameSinkFunc) AdmitRoomFrames(ctx context.Context, frames []RoomFrame) error {
	if f == nil {
		return ErrRoomFrameSinkRequired
	}
	return f(ctx, frames)
}

type roomSubscriberKey struct {
	roomID     int64
	subscriber coreentitysync.SubscriberRef
}

type roomFrameGroupKey struct {
	roomID     int64
	subscriber coreentitysync.SubscriberRef
}

// RoomEnvelopeSink adapts core subscription envelopes into room frames. An
// admission is serialized so frame/session counters can be committed only
// after the downstream sink has accepted the complete batch.
type RoomEnvelopeSink struct {
	admitMu stdsync.Mutex
	mu      stdsync.RWMutex

	downstream       ReliableRoomFrameSink
	subjectRooms     map[int64]int64
	roomFrames       map[int64]uint64
	sessionSequences map[roomSubscriberKey]uint64

	admittedFrames  atomic.Uint64
	admittedEntries atomic.Uint64
	failedBatches   atomic.Uint64
}

type RoomReplicationStats struct {
	AdmittedFrames     uint64
	AdmittedEntries    uint64
	FailedBatches      uint64
	FlushFailures      uint64
	PendingSubjects    int
	PendingRetirements int
	LastError          string
	ActiveSubjects     int
	ActiveSubscribers  int
	SessionSequences   int
}

func NewRoomEnvelopeSink(downstream ReliableRoomFrameSink) *RoomEnvelopeSink {
	return &RoomEnvelopeSink{
		downstream:       downstream,
		subjectRooms:     make(map[int64]int64),
		roomFrames:       make(map[int64]uint64),
		sessionSequences: make(map[roomSubscriberKey]uint64),
	}
}

func (s *RoomEnvelopeSink) SetDownstream(downstream ReliableRoomFrameSink) {
	if s == nil {
		return
	}
	s.admitMu.Lock()
	s.mu.Lock()
	s.downstream = downstream
	s.mu.Unlock()
	s.admitMu.Unlock()
}

func (s *RoomEnvelopeSink) RegisterSubject(roomID, subjectID int64) error {
	if s == nil || roomID == 0 {
		return ErrRoomIDInvalid
	}
	if subjectID == 0 {
		return ErrRoomSubjectInvalid
	}
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.subjectRooms[subjectID]; ok {
		if existing == roomID {
			return nil
		}
		return fmt.Errorf("%w: subject %d belongs to room %d", ErrRoomSubjectAlreadyExists, subjectID, existing)
	}
	s.subjectRooms[subjectID] = roomID
	return nil
}

func (s *RoomEnvelopeSink) UnregisterSubject(roomID, subjectID int64) error {
	if s == nil || roomID == 0 {
		return ErrRoomIDInvalid
	}
	if subjectID == 0 {
		return ErrRoomSubjectInvalid
	}
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	s.mu.Lock()
	existing, ok := s.subjectRooms[subjectID]
	if !ok || existing != roomID {
		s.mu.Unlock()
		return ErrRoomSubjectNotRegistered
	}
	delete(s.subjectRooms, subjectID)
	downstream := s.downstream
	s.mu.Unlock()
	if lifecycle, ok := downstream.(RoomSubjectLifecycle); ok {
		lifecycle.ReleaseRoomSubject(roomID, subjectID)
	}
	return nil
}

func (s *RoomEnvelopeSink) Stats() RoomReplicationStats {
	if s == nil {
		return RoomReplicationStats{}
	}
	s.mu.RLock()
	sequences := len(s.sessionSequences)
	s.mu.RUnlock()
	return RoomReplicationStats{
		AdmittedFrames:   s.admittedFrames.Load(),
		AdmittedEntries:  s.admittedEntries.Load(),
		FailedBatches:    s.failedBatches.Load(),
		SessionSequences: sequences,
	}
}

func (s *RoomEnvelopeSink) ReleaseSubscriber(ctx context.Context, roomID int64, subscriber coreentitysync.SubscriberRef) {
	if s == nil || roomID == 0 || subscriber.Normalize().Empty() {
		return
	}
	subscriber = subscriber.Normalize()
	s.admitMu.Lock()
	s.mu.Lock()
	delete(s.sessionSequences, roomSubscriberKey{roomID: roomID, subscriber: subscriber})
	downstream := s.downstream
	s.mu.Unlock()
	s.admitMu.Unlock()
	if lifecycle, ok := downstream.(RoomSubscriberLifecycle); ok {
		lifecycle.ReleaseRoomSubscriber(ctx, roomID, subscriber)
	}
}

func (s *RoomEnvelopeSink) ResetRoom(roomID int64) {
	if s == nil || roomID == 0 {
		return
	}
	s.admitMu.Lock()
	s.mu.Lock()
	delete(s.roomFrames, roomID)
	for key := range s.sessionSequences {
		if key.roomID == roomID {
			delete(s.sessionSequences, key)
		}
	}
	downstream := s.downstream
	s.mu.Unlock()
	s.admitMu.Unlock()
	if lifecycle, ok := downstream.(RoomLifecycle); ok {
		lifecycle.ResetRoom(roomID)
	}
}

func (s *RoomEnvelopeSink) AdmitEnvelopes(ctx context.Context, envelopes []coreentitysync.DeliveryEnvelope) error {
	if s == nil {
		return ErrRoomFrameSinkRequired
	}
	if len(envelopes) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.admitMu.Lock()
	defer s.admitMu.Unlock()

	s.mu.RLock()
	downstream := s.downstream
	groups := make(map[roomFrameGroupKey][]RoomFrameEntry)
	for _, envelope := range envelopes {
		subscriber := envelope.Subscriber.Normalize()
		roomID, registered := s.subjectRooms[envelope.Update.SubjectID]
		if subscriber.Empty() || envelope.Update.SubjectID == 0 || !registered {
			s.mu.RUnlock()
			return fmt.Errorf("%w: subject=%d subscriber=%+v", ErrRoomEnvelopeInvalid, envelope.Update.SubjectID, subscriber)
		}
		key := roomFrameGroupKey{roomID: roomID, subscriber: subscriber}
		groups[key] = append(groups[key], RoomFrameEntry{Kind: envelope.Kind, Update: envelope.Update})
	}
	if downstream == nil {
		s.mu.RUnlock()
		return ErrRoomFrameSinkRequired
	}

	keys := make([]roomFrameGroupKey, 0, len(groups))
	roomNext := make(map[int64]uint64)
	sessionNext := make(map[roomSubscriberKey]uint64, len(groups))
	for key := range groups {
		keys = append(keys, key)
		if _, ok := roomNext[key.roomID]; !ok {
			roomNext[key.roomID] = s.roomFrames[key.roomID] + 1
		}
		sessionKey := roomSubscriberKey{roomID: key.roomID, subscriber: key.subscriber}
		sessionNext[sessionKey] = s.sessionSequences[sessionKey] + 1
	}
	s.mu.RUnlock()

	sort.Slice(keys, func(i, j int) bool { return lessRoomFrameGroupKey(keys[i], keys[j]) })
	frames := make([]RoomFrame, 0, len(keys))
	entryCount := 0
	for _, key := range keys {
		entries := groups[key]
		sort.SliceStable(entries, func(i, j int) bool {
			if entries[i].Update.SubjectID != entries[j].Update.SubjectID {
				return entries[i].Update.SubjectID < entries[j].Update.SubjectID
			}
			return entries[i].Kind < entries[j].Kind
		})
		sessionKey := roomSubscriberKey{roomID: key.roomID, subscriber: key.subscriber}
		frames = append(frames, RoomFrame{
			RoomID: key.roomID, Frame: roomNext[key.roomID], Subscriber: key.subscriber,
			SessionSequence: sessionNext[sessionKey], Entries: entries,
		})
		entryCount += len(entries)
	}

	if err := admitRoomFrames(ctx, downstream, frames); err != nil {
		s.failedBatches.Add(1)
		return errors.Join(ErrRoomFrameAdmission, err)
	}

	s.mu.Lock()
	for roomID, frame := range roomNext {
		s.roomFrames[roomID] = frame
	}
	for key, sequence := range sessionNext {
		s.sessionSequences[key] = sequence
	}
	s.mu.Unlock()
	s.admittedFrames.Add(uint64(len(frames)))
	s.admittedEntries.Add(uint64(entryCount))
	return nil
}

func lessRoomFrameGroupKey(a, b roomFrameGroupKey) bool {
	if a.roomID != b.roomID {
		return a.roomID < b.roomID
	}
	left := a.subscriber.Normalize()
	right := b.subscriber.Normalize()
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	if left.ID != right.ID {
		return left.ID < right.ID
	}
	if left.Sid != right.Sid {
		return left.Sid < right.Sid
	}
	return left.Key < right.Key
}

func admitRoomFrames(ctx context.Context, sink ReliableRoomFrameSink, frames []RoomFrame) (err error) {
	if sink == nil {
		return ErrRoomFrameSinkRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: sink panic: %v", ErrRoomFrameAdmission, recovered)
		}
	}()
	return sink.AdmitRoomFrames(ctx, frames)
}

// RoomReplication is the application-facing facade for one room. Application
// code registers entity content state, then only subscribes, unsubscribes and
// flushes; it never owns entity locks or observer collections.
type RoomReplication struct {
	roomID       int64
	envelopeSink *RoomEnvelopeSink
	coordinator  *coreentitysync.SubscriptionCoordinator

	mu             stdsync.RWMutex
	subjects       map[int64]*entity.SubjectSyncState
	subscribers    map[coreentitysync.SubscriberRef]int
	maxSubjects    int
	maxSubscribers int
	started        bool
	stopped        bool
	stopCh         chan struct{}
	doneCh         chan struct{}
	lastError      error

	dirtyMu  stdsync.Mutex
	dirty    map[int64]struct{}
	retiring map[int64]struct{}

	flushOps      [64]stdsync.Mutex
	flushFailures atomic.Uint64
}

type RoomReplicationConfig struct {
	MaxSubjects    int
	MaxSubscribers int
}

func NewRoomReplication(roomID int64, downstream ReliableRoomFrameSink, configs ...RoomReplicationConfig) (*RoomReplication, error) {
	if roomID == 0 {
		return nil, ErrRoomIDInvalid
	}
	if downstream == nil {
		return nil, ErrRoomFrameSinkRequired
	}
	envelopeSink := NewRoomEnvelopeSink(downstream)
	config := RoomReplicationConfig{MaxSubjects: 100, MaxSubscribers: 100}
	if len(configs) > 0 {
		if configs[0].MaxSubjects > 0 {
			config.MaxSubjects = configs[0].MaxSubjects
		}
		if configs[0].MaxSubscribers > 0 {
			config.MaxSubscribers = configs[0].MaxSubscribers
		}
	}
	replication := &RoomReplication{
		roomID: roomID, envelopeSink: envelopeSink,
		coordinator: coreentitysync.NewSubscriptionCoordinator(envelopeSink),
		subjects:    make(map[int64]*entity.SubjectSyncState),
		subscribers: make(map[coreentitysync.SubscriberRef]int),
		maxSubjects: config.MaxSubjects, maxSubscribers: config.MaxSubscribers,
		dirty:    make(map[int64]struct{}),
		retiring: make(map[int64]struct{}),
	}
	if lifecycle, ok := downstream.(interface{ SetRoomSlowConsumerHandler(func(RoomSlowConsumer)) }); ok {
		lifecycle.SetRoomSlowConsumerHandler(replication.handleSlowConsumer)
	}
	return replication, nil
}

func (r *RoomReplication) handleSlowConsumer(event RoomSlowConsumer) {
	if r == nil || event.RoomID != r.roomID || event.Subscriber.Normalize().Empty() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	subscriber := event.Subscriber.Normalize()
	r.mu.RLock()
	subjectIDs := make([]int64, 0, len(r.subjects))
	for subjectID := range r.subjects {
		subjectIDs = append(subjectIDs, subjectID)
	}
	r.mu.RUnlock()
	for _, subjectID := range subjectIDs {
		if !containsRoomSubscriber(r.coordinator.Subscribers(subjectID), subscriber) {
			continue
		}
		err := r.Unsubscribe(ctx, subscriber, subjectID)
		if err != nil && !errors.Is(err, coreentitysync.ErrSubscriptionNotFound) && !errors.Is(err, ErrRoomSubjectNotRegistered) {
			r.setLastError(errors.Join(r.LastError(), fmt.Errorf("sync: evict slow subscriber: %w", err)))
		}
	}
}

func (r *RoomReplication) RoomID() int64 {
	if r == nil {
		return 0
	}
	return r.roomID
}

func (r *RoomReplication) SetDownstream(downstream ReliableRoomFrameSink) error {
	if r == nil {
		return ErrRoomIDInvalid
	}
	if downstream == nil {
		return ErrRoomFrameSinkRequired
	}
	r.envelopeSink.SetDownstream(downstream)
	return nil
}

func (r *RoomReplication) RegisterSubject(state *entity.SubjectSyncState) error {
	if r == nil || state == nil || !state.Enabled() || state.SubjectID() == 0 {
		return ErrRoomSubjectInvalid
	}
	subjectID := state.SubjectID()
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrRoomReplicationStopped
	}
	if existing, ok := r.subjects[subjectID]; ok {
		r.mu.Unlock()
		if existing == state {
			return nil
		}
		return fmt.Errorf("%w: subject %d", ErrRoomSubjectAlreadyExists, subjectID)
	}
	if len(r.subjects) >= r.maxSubjects {
		r.mu.Unlock()
		return ErrRoomSubjectLimit
	}
	if err := r.envelopeSink.RegisterSubject(r.roomID, subjectID); err != nil {
		r.mu.Unlock()
		return err
	}
	r.subjects[subjectID] = state
	r.mu.Unlock()
	state.SetDirtyNotifier(r.markDirty)
	return nil
}

func (r *RoomReplication) UnregisterSubject(subjectID int64) error {
	if r == nil || subjectID == 0 {
		return ErrRoomSubjectInvalid
	}
	op := r.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()
	return r.unregisterSubject(subjectID)
}

func (r *RoomReplication) unregisterSubject(subjectID int64) error {
	if r == nil || subjectID == 0 {
		return ErrRoomSubjectInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.subjects[subjectID]; !ok {
		return ErrRoomSubjectNotRegistered
	}
	if len(r.coordinator.Subscribers(subjectID)) != 0 {
		return ErrRoomSubjectHasSubscribers
	}
	if err := r.envelopeSink.UnregisterSubject(r.roomID, subjectID); err != nil {
		return err
	}
	r.subjects[subjectID].SetDirtyNotifier(nil)
	delete(r.subjects, subjectID)
	r.clearDirty(subjectID)
	r.clearRetiring(subjectID)
	return nil
}

func (r *RoomReplication) Subscribe(ctx context.Context, subscriber coreentitysync.SubscriberRef, subjectID int64, profile entity.SyncProfile) (coreentitysync.Subscription, error) {
	if r == nil || subjectID == 0 {
		return coreentitysync.Subscription{}, ErrRoomSubjectInvalid
	}
	op := r.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()
	if r.isRetiring(subjectID) {
		return coreentitysync.Subscription{}, ErrRoomSubjectRetiring
	}
	state, err := r.subject(subjectID)
	if err != nil {
		return coreentitysync.Subscription{}, err
	}
	subscriber = subscriber.Normalize()
	already := containsRoomSubscriber(r.coordinator.Subscribers(subjectID), subscriber)
	if !already {
		r.mu.Lock()
		if r.subscribers[subscriber] == 0 && len(r.subscribers) >= r.maxSubscribers {
			r.mu.Unlock()
			return coreentitysync.Subscription{}, ErrRoomSubscriberLimit
		}
		r.subscribers[subscriber]++
		r.mu.Unlock()
	}
	subscription, err := r.coordinator.Subscribe(ctx, subscriber, state, profile)
	if err != nil && !already {
		r.releaseSubscriber(ctx, subscriber)
	}
	return subscription, err
}

// RetireSubject prevents new subscriptions immediately, transactionally
// admits leave frames for current subscribers, and unregisters the subject.
// Failures remain queued and are retried by the room worker.
func (r *RoomReplication) RetireSubject(ctx context.Context, subjectID int64) error {
	if _, err := r.subject(subjectID); err != nil {
		return err
	}
	r.dirtyMu.Lock()
	r.retiring[subjectID] = struct{}{}
	delete(r.dirty, subjectID)
	r.dirtyMu.Unlock()
	err := r.retryRetirement(ctx, subjectID)
	if err != nil {
		r.flushFailures.Add(1)
	}
	return err
}

func (r *RoomReplication) Unsubscribe(ctx context.Context, subscriber coreentitysync.SubscriberRef, subjectID int64) error {
	if r == nil || subjectID == 0 {
		return ErrRoomSubjectInvalid
	}
	op := r.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()
	if _, err := r.subject(subjectID); err != nil {
		return err
	}
	return r.unsubscribeTracked(ctx, subscriber.Normalize(), subjectID)
}

func (r *RoomReplication) unsubscribeTracked(ctx context.Context, subscriber coreentitysync.SubscriberRef, subjectID int64) error {
	if err := r.coordinator.Unsubscribe(ctx, subscriber, subjectID); err != nil {
		return err
	}
	r.releaseSubscriber(ctx, subscriber)
	return nil
}

func (r *RoomReplication) releaseSubscriber(ctx context.Context, subscriber coreentitysync.SubscriberRef) {
	r.mu.Lock()
	count := r.subscribers[subscriber]
	if count <= 1 {
		delete(r.subscribers, subscriber)
	} else {
		r.subscribers[subscriber] = count - 1
	}
	released := count == 1
	r.mu.Unlock()
	if released {
		r.envelopeSink.ReleaseSubscriber(ctx, r.roomID, subscriber)
	}
}

func containsRoomSubscriber(items []coreentitysync.Subscription, target coreentitysync.SubscriberRef) bool {
	for _, item := range items {
		if item.Subscriber.Normalize() == target {
			return true
		}
	}
	return false
}

func (r *RoomReplication) FlushSubject(ctx context.Context, subjectID int64) error {
	state, err := r.subject(subjectID)
	if err != nil {
		return err
	}
	r.clearDirty(subjectID)
	return r.flushState(ctx, state)
}

// Start enables coalesced asynchronous flushes. The default interval is 50ms
// (20Hz). Dirty notification only schedules work; packing remains protected by
// the Entity mutex inside core.
func (r *RoomReplication) Start(ctx context.Context, interval time.Duration) error {
	if r == nil {
		return ErrRoomIDInvalid
	}
	if interval <= 0 {
		interval = DefaultRoomReplicationInterval
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return ErrRoomReplicationStopped
	}
	if r.started {
		r.mu.Unlock()
		return nil
	}
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	r.stopCh = stopCh
	r.doneCh = doneCh
	r.started = true
	r.mu.Unlock()
	go r.run(ctx, interval, stopCh, doneCh)
	return nil
}

// Stop flushes admitted dirty subjects once and waits for the worker. A room
// replication instance is intentionally not restartable after Stop.
func (r *RoomReplication) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.stopped {
		doneCh := r.doneCh
		r.mu.Unlock()
		if doneCh == nil {
			return nil
		}
		select {
		case <-doneCh:
			r.detachNotifiers()
			return r.LastError()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.stopped = true
	started := r.started
	stopCh := r.stopCh
	doneCh := r.doneCh
	r.mu.Unlock()
	if !started {
		err := r.FlushDirty(ctx)
		r.detachNotifiers()
		return err
	}
	close(stopCh)
	select {
	case <-doneCh:
		r.detachNotifiers()
		return r.LastError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FlushDirty is the active flush API. It drains the current dirty set in
// stable subject order and requeues every state that remains dirty on failure
// or because a concurrent mutation arrived during delivery.
func (r *RoomReplication) FlushDirty(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ids := r.takeDirty()
	var flushErrors []error
	for _, subjectID := range ids {
		if r.isRetiring(subjectID) {
			continue
		}
		state, err := r.subject(subjectID)
		if errors.Is(err, ErrRoomSubjectNotRegistered) {
			continue
		}
		if err == nil {
			err = r.flushState(ctx, state)
		}
		if err != nil {
			r.flushFailures.Add(1)
			flushErrors = append(flushErrors, fmt.Errorf("subject %d: %w", subjectID, err))
		}
		if state != nil && state.PendingDirty() {
			r.markDirty(state)
		}
	}
	for _, subjectID := range r.retirementIDs() {
		if err := r.retryRetirement(ctx, subjectID); err != nil {
			r.flushFailures.Add(1)
			flushErrors = append(flushErrors, fmt.Errorf("retire subject %d: %w", subjectID, err))
		}
	}
	return errors.Join(flushErrors...)
}

func (r *RoomReplication) Subscribers(subjectID int64) []coreentitysync.Subscription {
	if r == nil {
		return nil
	}
	return r.coordinator.Subscribers(subjectID)
}

func (r *RoomReplication) Profiles(subjectID int64) []entity.SyncProfile {
	if r == nil {
		return nil
	}
	return r.coordinator.Profiles(subjectID)
}

func (r *RoomReplication) Stats() RoomReplicationStats {
	if r == nil {
		return RoomReplicationStats{}
	}
	stats := r.envelopeSink.Stats()
	stats.FlushFailures = r.flushFailures.Load()
	r.dirtyMu.Lock()
	stats.PendingSubjects = len(r.dirty)
	stats.PendingRetirements = len(r.retiring)
	r.dirtyMu.Unlock()
	r.mu.RLock()
	stats.ActiveSubjects = len(r.subjects)
	stats.ActiveSubscribers = len(r.subscribers)
	r.mu.RUnlock()
	if err := r.LastError(); err != nil {
		stats.LastError = err.Error()
	}
	return stats
}

func (r *RoomReplication) LastError() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	err := r.lastError
	r.mu.RUnlock()
	return err
}

func (r *RoomReplication) setLastError(err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.lastError = err
	r.mu.Unlock()
}

func (r *RoomReplication) subject(subjectID int64) (*entity.SubjectSyncState, error) {
	if r == nil || subjectID == 0 {
		return nil, ErrRoomSubjectInvalid
	}
	r.mu.RLock()
	state, ok := r.subjects[subjectID]
	r.mu.RUnlock()
	if !ok {
		return nil, ErrRoomSubjectNotRegistered
	}
	return state, nil
}

func (r *RoomReplication) flushState(ctx context.Context, state *entity.SubjectSyncState) error {
	if state == nil {
		return ErrRoomSubjectInvalid
	}
	subjectID := state.SubjectID()
	op := &r.flushOps[uint64(subjectID)%uint64(len(r.flushOps))]
	op.Lock()
	defer op.Unlock()
	err := r.coordinator.FlushSubject(ctx, state)
	if state.PendingDirty() {
		r.markDirty(state)
	}
	return err
}

func (r *RoomReplication) markDirty(state *entity.SubjectSyncState) {
	if r == nil || state == nil || state.SubjectID() == 0 {
		return
	}
	r.mu.RLock()
	registered := r.subjects[state.SubjectID()] == state
	r.mu.RUnlock()
	if !registered || r.isRetiring(state.SubjectID()) {
		return
	}
	r.dirtyMu.Lock()
	r.dirty[state.SubjectID()] = struct{}{}
	r.dirtyMu.Unlock()
}

func (r *RoomReplication) retryRetirement(ctx context.Context, subjectID int64) error {
	op := r.subjectOp(subjectID)
	op.Lock()
	defer op.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	subscriptions := r.coordinator.Subscribers(subjectID)
	var retireErrors []error
	for _, subscription := range subscriptions {
		if err := r.unsubscribeTracked(ctx, subscription.Subscriber.Normalize(), subjectID); err != nil && !errors.Is(err, coreentitysync.ErrSubscriptionNotFound) {
			retireErrors = append(retireErrors, err)
		}
	}
	if err := errors.Join(retireErrors...); err != nil {
		return err
	}
	if err := r.unregisterSubject(subjectID); err != nil && !errors.Is(err, ErrRoomSubjectNotRegistered) {
		return err
	}
	r.clearRetiring(subjectID)
	return nil
}

func (r *RoomReplication) subjectOp(subjectID int64) *stdsync.Mutex {
	return &r.flushOps[uint64(subjectID)%uint64(len(r.flushOps))]
}

func (r *RoomReplication) isRetiring(subjectID int64) bool {
	r.dirtyMu.Lock()
	_, ok := r.retiring[subjectID]
	r.dirtyMu.Unlock()
	return ok
}

func (r *RoomReplication) clearRetiring(subjectID int64) {
	r.dirtyMu.Lock()
	delete(r.retiring, subjectID)
	r.dirtyMu.Unlock()
}

func (r *RoomReplication) retirementIDs() []int64 {
	r.dirtyMu.Lock()
	ids := make([]int64, 0, len(r.retiring))
	for subjectID := range r.retiring {
		ids = append(ids, subjectID)
	}
	r.dirtyMu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (r *RoomReplication) detachNotifiers() {
	r.mu.Lock()
	states := make([]*entity.SubjectSyncState, 0, len(r.subjects))
	for _, state := range r.subjects {
		states = append(states, state)
	}
	clear(r.subscribers)
	r.mu.Unlock()
	for _, state := range states {
		state.SetDirtyNotifier(nil)
	}
	r.envelopeSink.ResetRoom(r.roomID)
}

func (r *RoomReplication) clearDirty(subjectID int64) {
	r.dirtyMu.Lock()
	delete(r.dirty, subjectID)
	r.dirtyMu.Unlock()
}

func (r *RoomReplication) takeDirty() []int64 {
	r.dirtyMu.Lock()
	ids := make([]int64, 0, len(r.dirty))
	for subjectID := range r.dirty {
		ids = append(ids, subjectID)
		delete(r.dirty, subjectID)
	}
	r.dirtyMu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (r *RoomReplication) run(ctx context.Context, interval time.Duration, stopCh <-chan struct{}, doneCh chan<- struct{}) {
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		flushCtx, cancel := context.WithTimeout(context.Background(), interval)
		err := r.FlushDirty(flushCtx)
		cancel()
		r.mu.Lock()
		r.lastError = err
		if r.doneCh == doneCh {
			r.started = false
		}
		r.mu.Unlock()
		close(doneCh)
	}()
	for {
		select {
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(context.Background(), interval)
			err := r.FlushDirty(flushCtx)
			cancel()
			r.mu.Lock()
			r.lastError = err
			r.mu.Unlock()
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		}
	}
}
