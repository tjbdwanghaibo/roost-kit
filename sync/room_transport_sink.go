package sync

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	stdsync "sync"

	"github.com/tjbdwanghaibo/cube-core/entity"
	coreentitysync "github.com/tjbdwanghaibo/cube-core/entitysync"
	core "github.com/tjbdwanghaibo/cube-core/replication"
	kit "github.com/tjbdwanghaibo/cube-kit/replication"
)

const (
	roomWireMagic          uint32 = 0x52524631 // RRF1
	roomSubjectMagic       uint32 = 0x52535531 // RSU1
	RoomWireVersion        uint16 = 1
	DefaultRoomComponentID uint16 = 1
	DefaultRoomArchetype   uint16 = 1
	roomWireHeaderBytes           = 28
	roomSubjectHeaderBytes        = 64
	roomSubjectFlagFull    uint16 = 1 << iota
)

var (
	ErrRoomTransportRequired = errors.New("sync: atomic room transport is required")
	ErrRoomSessionResolver   = errors.New("sync: room session resolver is required")
	ErrRoomSessionInvalid    = errors.New("sync: resolved room session is invalid")
	ErrRoomWireFrame         = errors.New("sync: invalid room wire frame")
	ErrRoomSubjectBaseline   = errors.New("sync: room subject has no transport baseline")
)

type RoomSessionResolver interface {
	ResolveRoomSession(context.Context, coreentitysync.SubscriberRef) (core.SessionID, error)
}

type RoomSessionResolverFunc func(context.Context, coreentitysync.SubscriberRef) (core.SessionID, error)

func (f RoomSessionResolverFunc) ResolveRoomSession(ctx context.Context, subscriber coreentitysync.SubscriberRef) (core.SessionID, error) {
	if f == nil {
		return 0, ErrRoomSessionResolver
	}
	return f(ctx, subscriber)
}

type RoomTransportSinkConfig struct {
	Transport              kit.AtomicBatchTransport
	Sessions               RoomSessionResolver
	Limits                 core.Limits
	MaxDatagramBytes       int
	FrameSchemaVersion     uint16
	ComponentTypeID        uint16
	ComponentSchemaVersion uint16
	Archetype              uint16
}

// RoomTransportSink encodes receiver-specific room frames onto the common
// replication wire format. Snapshot/leave frames use the reliable ordered
// lane; state-only deltas use fragmented latest-only datagrams.
type RoomTransportSink struct {
	mu        stdsync.Mutex
	transport kit.AtomicBatchTransport
	sessions  RoomSessionResolver
	config    RoomTransportSinkConfig
	rooms     map[roomSessionKey]*roomObjectRefs
}

type roomSessionKey struct {
	roomID  uint64
	session core.SessionID
}

type roomObjectRefs struct {
	objects     map[int64]core.ObjectRef
	generations map[uint16]uint16
	free        []uint16
	next        uint16
}

func NewRoomTransportSink(config RoomTransportSinkConfig) (*RoomTransportSink, error) {
	if config.Transport == nil {
		return nil, ErrRoomTransportRequired
	}
	if config.Sessions == nil {
		return nil, ErrRoomSessionResolver
	}
	defaults := core.DefaultLimits()
	config.Limits = mergeRoomLimits(config.Limits, defaults)
	if config.MaxDatagramBytes <= 0 {
		config.MaxDatagramBytes = config.Limits.MaxDatagramBytes
	}
	if config.MaxDatagramBytes > config.Limits.MaxDatagramBytes || config.MaxDatagramBytes <= core.DatagramHeaderSize {
		return nil, core.ErrInvalidDatagram
	}
	if config.FrameSchemaVersion == 0 {
		config.FrameSchemaVersion = RoomWireVersion
	}
	if config.ComponentTypeID == 0 {
		config.ComponentTypeID = DefaultRoomComponentID
	}
	if config.ComponentSchemaVersion == 0 {
		config.ComponentSchemaVersion = RoomWireVersion
	}
	if config.Archetype == 0 {
		config.Archetype = DefaultRoomArchetype
	}
	return &RoomTransportSink{
		transport: config.Transport, sessions: config.Sessions, config: config,
		rooms: make(map[roomSessionKey]*roomObjectRefs),
	}, nil
}

func (s *RoomTransportSink) AdmitRoomFrames(ctx context.Context, frames []RoomFrame) error {
	if s == nil || s.transport == nil {
		return ErrRoomTransportRequired
	}
	if len(frames) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	plans := make(map[roomSessionKey]*roomObjectRefs)
	seenRoutes := make(map[roomSessionKey]struct{}, len(frames))
	outbound := make([]kit.OutboundFrame, 0, len(frames))
	for _, frame := range frames {
		if frame.RoomID <= 0 || frame.Frame == 0 || frame.SessionSequence == 0 || frame.Subscriber.Normalize().Empty() || len(frame.Entries) == 0 {
			return ErrRoomWireFrame
		}
		session, err := s.sessions.ResolveRoomSession(ctx, frame.Subscriber.Normalize())
		if err != nil {
			return errors.Join(ErrRoomSessionInvalid, err)
		}
		if session == 0 {
			return ErrRoomSessionInvalid
		}
		roomID := uint64(frame.RoomID)
		key := roomSessionKey{roomID: roomID, session: session}
		if _, exists := seenRoutes[key]; exists {
			return fmt.Errorf("%w: duplicate room/session route", ErrRoomWireFrame)
		}
		seenRoutes[key] = struct{}{}
		state := plans[key]
		if state == nil {
			state = cloneRoomObjectRefs(s.rooms[key])
			plans[key] = state
		}
		encoded, reliable, delta, err := s.encodeFrame(frame, state)
		if err != nil {
			return err
		}
		item := kit.OutboundFrame{Session: session}
		if reliable {
			item.Reliable = encoded
		} else {
			sequence := nonzeroSequence(frame.SessionSequence)
			item.Datagrams, err = core.FragmentFrame(delta, sequence, encoded, s.config.MaxDatagramBytes, s.config.Limits)
			if err != nil {
				return err
			}
		}
		outbound = append(outbound, item)
	}
	if err := s.transport.AdmitBatch(ctx, outbound); err != nil {
		return err
	}
	for key, state := range plans {
		s.rooms[key] = state
	}
	return nil
}

func (s *RoomTransportSink) encodeFrame(frame RoomFrame, state *roomObjectRefs) ([]byte, bool, core.DeltaFrame, error) {
	epoch, tick, baseTick := roomWireClock(frame.Frame)
	delta := core.DeltaFrame{
		SnapshotMeta: core.SnapshotMeta{
			RoomID: uint64(frame.RoomID), Epoch: epoch, Tick: tick,
			SchemaVersion: s.config.FrameSchemaVersion,
		},
		Kind: core.FrameDelta, BaseTick: baseTick,
		Objects: make([]core.ObjectDelta, 0, len(frame.Entries)),
	}
	if len(frame.Entries) > s.config.Limits.MaxObjects {
		return nil, false, core.DeltaFrame{}, core.ErrObjectLimit
	}
	seen := make(map[int64]struct{}, len(frame.Entries))
	reliable := false
	for _, entry := range frame.Entries {
		update := entry.Update
		if update.SubjectID == 0 {
			return nil, false, core.DeltaFrame{}, ErrRoomSubjectInvalid
		}
		if _, exists := seen[update.SubjectID]; exists {
			return nil, false, core.DeltaFrame{}, fmt.Errorf("%w: duplicate subject %d", ErrRoomWireFrame, update.SubjectID)
		}
		seen[update.SubjectID] = struct{}{}
		ref, exists := state.objects[update.SubjectID]
		object := core.ObjectDelta{Ref: ref}
		switch entry.Kind {
		case coreentitysync.EnvelopeSnapshot:
			reliable = true
			if !exists {
				var err error
				ref, err = state.allocate(update.SubjectID, s.config.Limits.MaxObjects)
				if err != nil {
					return nil, false, core.DeltaFrame{}, err
				}
			}
			object.Ref = ref
			object.Operation = core.ObjectCreate
			object.Archetype = s.config.Archetype
		case coreentitysync.EnvelopeDelta:
			if !exists {
				return nil, false, core.DeltaFrame{}, fmt.Errorf("%w: room=%d subject=%d", ErrRoomSubjectBaseline, frame.RoomID, update.SubjectID)
			}
			object.Operation = core.ObjectUpdate
		case coreentitysync.EnvelopeLeave:
			reliable = true
			if !exists {
				return nil, false, core.DeltaFrame{}, fmt.Errorf("%w: room=%d subject=%d", ErrRoomSubjectBaseline, frame.RoomID, update.SubjectID)
			}
			object.Operation = core.ObjectRemove
		default:
			return nil, false, core.DeltaFrame{}, ErrRoomWireFrame
		}
		if object.Operation != core.ObjectRemove {
			payload, err := EncodeRoomSubjectUpdate(update, s.config.Limits.MaxComponentBytes)
			if err != nil {
				return nil, false, core.DeltaFrame{}, err
			}
			object.Components = []core.ComponentDelta{{
				Operation: core.ComponentSet, TypeID: s.config.ComponentTypeID,
				SchemaVersion: s.config.ComponentSchemaVersion, Data: payload,
			}}
		}
		delta.Objects = append(delta.Objects, object)
	}
	inner, err := core.EncodeFrame(delta, s.config.Limits)
	if err != nil {
		return nil, false, core.DeltaFrame{}, err
	}
	return encodeRoomWireFrame(frame.Frame, frame.SessionSequence, inner), reliable, delta, nil
}

func (s *RoomTransportSink) ReleaseRoomSubject(roomID, subjectID int64) {
	if s == nil || roomID <= 0 || subjectID == 0 {
		return
	}
	s.mu.Lock()
	for key, state := range s.rooms {
		if key.roomID == uint64(roomID) && state != nil {
			state.release(subjectID)
		}
	}
	s.mu.Unlock()
}

func (s *RoomTransportSink) ResetRoom(roomID int64) {
	if s == nil || roomID <= 0 {
		return
	}
	s.mu.Lock()
	for key := range s.rooms {
		if key.roomID == uint64(roomID) {
			delete(s.rooms, key)
		}
	}
	s.mu.Unlock()
}

func (s *RoomTransportSink) Decode(data []byte) (uint64, uint64, core.DeltaFrame, error) {
	if s == nil {
		return 0, 0, core.DeltaFrame{}, ErrRoomWireFrame
	}
	return DecodeRoomWireFrame(data, s.config.Limits)
}

func EncodeRoomSubjectUpdate(update entity.SubjectSyncUpdate, maxBytes int) ([]byte, error) {
	profile := update.Profile.Normalize()
	namespace := []byte(update.Namespace)
	profileKey := []byte(profile.Key)
	if len(namespace) > int(^uint16(0)) || len(profileKey) > int(^uint16(0)) {
		return nil, core.ErrComponentTooLarge
	}
	total := roomSubjectHeaderBytes + len(namespace) + len(profileKey) + update.Payload.Len()
	if maxBytes <= 0 {
		maxBytes = core.DefaultLimits().MaxComponentBytes
	}
	if total > maxBytes || update.SubjectID == 0 {
		return nil, core.ErrComponentTooLarge
	}
	out := make([]byte, roomSubjectHeaderBytes, total)
	binary.BigEndian.PutUint32(out[0:4], roomSubjectMagic)
	binary.BigEndian.PutUint16(out[4:6], RoomWireVersion)
	flags := uint16(0)
	if update.Full {
		flags |= roomSubjectFlagFull
	}
	binary.BigEndian.PutUint16(out[6:8], flags)
	binary.BigEndian.PutUint64(out[8:16], uint64(update.SubjectID))
	binary.BigEndian.PutUint32(out[16:20], update.SubjectKind)
	binary.BigEndian.PutUint64(out[20:28], update.Version)
	binary.BigEndian.PutUint64(out[28:36], update.BaseVersion)
	binary.BigEndian.PutUint64(out[36:44], update.Mask)
	binary.BigEndian.PutUint32(out[44:48], update.Reason)
	binary.BigEndian.PutUint16(out[48:50], update.Payload.Codec())
	out[50] = profile.LOD
	binary.BigEndian.PutUint32(out[52:56], profile.SchemaVersion)
	binary.BigEndian.PutUint16(out[56:58], uint16(len(namespace)))
	binary.BigEndian.PutUint16(out[58:60], uint16(len(profileKey)))
	binary.BigEndian.PutUint32(out[60:64], uint32(update.Payload.Len()))
	out = append(out, namespace...)
	out = append(out, profileKey...)
	out = update.Payload.AppendTo(out)
	return out, nil
}

func DecodeRoomSubjectUpdate(data []byte, maxBytes int) (entity.SubjectSyncUpdate, error) {
	if maxBytes <= 0 {
		maxBytes = core.DefaultLimits().MaxComponentBytes
	}
	if len(data) < roomSubjectHeaderBytes || len(data) > maxBytes || binary.BigEndian.Uint32(data[0:4]) != roomSubjectMagic || binary.BigEndian.Uint16(data[4:6]) != RoomWireVersion {
		return entity.SubjectSyncUpdate{}, ErrRoomWireFrame
	}
	flags := binary.BigEndian.Uint16(data[6:8])
	if flags & ^roomSubjectFlagFull != 0 || data[51] != 0 {
		return entity.SubjectSyncUpdate{}, ErrRoomWireFrame
	}
	namespaceLength := int(binary.BigEndian.Uint16(data[56:58]))
	profileLength := int(binary.BigEndian.Uint16(data[58:60]))
	payloadLength := int(binary.BigEndian.Uint32(data[60:64]))
	if roomSubjectHeaderBytes+namespaceLength+profileLength+payloadLength != len(data) {
		return entity.SubjectSyncUpdate{}, ErrRoomWireFrame
	}
	offset := roomSubjectHeaderBytes
	update := entity.SubjectSyncUpdate{
		SubjectID: int64(binary.BigEndian.Uint64(data[8:16])), SubjectKind: binary.BigEndian.Uint32(data[16:20]),
		Version: binary.BigEndian.Uint64(data[20:28]), BaseVersion: binary.BigEndian.Uint64(data[28:36]),
		Mask: binary.BigEndian.Uint64(data[36:44]), Reason: binary.BigEndian.Uint32(data[44:48]), Full: flags&roomSubjectFlagFull != 0,
		Namespace: string(data[offset : offset+namespaceLength]),
	}
	offset += namespaceLength
	update.Profile = entity.SyncProfile{Key: string(data[offset : offset+profileLength]), LOD: data[50], SchemaVersion: binary.BigEndian.Uint32(data[52:56])}.Normalize()
	offset += profileLength
	update.Payload = entity.CopyFrozenSyncPayload(binary.BigEndian.Uint16(data[48:50]), data[offset:offset+payloadLength])
	if update.SubjectID == 0 {
		return entity.SubjectSyncUpdate{}, ErrRoomWireFrame
	}
	return update, nil
}

func DecodeRoomWireFrame(data []byte, limits core.Limits) (uint64, uint64, core.DeltaFrame, error) {
	if len(data) < roomWireHeaderBytes || binary.BigEndian.Uint32(data[0:4]) != roomWireMagic || binary.BigEndian.Uint16(data[4:6]) != RoomWireVersion || binary.BigEndian.Uint16(data[6:8]) != 0 {
		return 0, 0, core.DeltaFrame{}, ErrRoomWireFrame
	}
	frame := binary.BigEndian.Uint64(data[8:16])
	sequence := binary.BigEndian.Uint64(data[16:24])
	innerLength := int(binary.BigEndian.Uint32(data[24:28]))
	if frame == 0 || sequence == 0 || innerLength != len(data)-roomWireHeaderBytes {
		return 0, 0, core.DeltaFrame{}, ErrRoomWireFrame
	}
	delta, err := core.DecodeFrame(data[roomWireHeaderBytes:], limits)
	if err != nil {
		return 0, 0, core.DeltaFrame{}, err
	}
	return frame, sequence, delta, nil
}

func encodeRoomWireFrame(frame, sequence uint64, inner []byte) []byte {
	out := make([]byte, roomWireHeaderBytes, roomWireHeaderBytes+len(inner))
	binary.BigEndian.PutUint32(out[0:4], roomWireMagic)
	binary.BigEndian.PutUint16(out[4:6], RoomWireVersion)
	binary.BigEndian.PutUint64(out[8:16], frame)
	binary.BigEndian.PutUint64(out[16:24], sequence)
	binary.BigEndian.PutUint32(out[24:28], uint32(len(inner)))
	return append(out, inner...)
}

func roomWireClock(frame uint64) (uint32, uint32, uint32) {
	const period = uint64(^uint32(0) - 1)
	value := frame - 1
	epoch := uint32(value/period) + 1
	tick := uint32(value%period) + 2
	return epoch, tick, tick - 1
}

func nonzeroSequence(sequence uint64) uint32 {
	return uint32((sequence-1)%uint64(^uint32(0))) + 1
}

func mergeRoomLimits(value, defaults core.Limits) core.Limits {
	if value.MaxObjects <= 0 {
		value.MaxObjects = defaults.MaxObjects
	}
	if value.MaxComponentsPerObject <= 0 {
		value.MaxComponentsPerObject = defaults.MaxComponentsPerObject
	}
	if value.MaxComponentBytes <= 0 {
		value.MaxComponentBytes = defaults.MaxComponentBytes
	}
	if value.MaxFrameBytes <= 0 {
		value.MaxFrameBytes = defaults.MaxFrameBytes
	}
	if value.MaxDatagramBytes <= 0 {
		value.MaxDatagramBytes = defaults.MaxDatagramBytes
	}
	if value.MaxFragments <= 0 {
		value.MaxFragments = defaults.MaxFragments
	}
	if value.MaxInflightFrames <= 0 {
		value.MaxInflightFrames = defaults.MaxInflightFrames
	}
	return value
}

func cloneRoomObjectRefs(source *roomObjectRefs) *roomObjectRefs {
	if source == nil {
		return &roomObjectRefs{objects: make(map[int64]core.ObjectRef), generations: make(map[uint16]uint16), next: 1}
	}
	clone := &roomObjectRefs{
		objects: make(map[int64]core.ObjectRef, len(source.objects)), generations: make(map[uint16]uint16, len(source.generations)),
		free: append([]uint16(nil), source.free...), next: source.next,
	}
	for id, ref := range source.objects {
		clone.objects[id] = ref
	}
	for id, generation := range source.generations {
		clone.generations[id] = generation
	}
	return clone
}

func (state *roomObjectRefs) allocate(subjectID int64, maxObjects int) (core.ObjectRef, error) {
	if len(state.objects) >= maxObjects {
		return core.ObjectRef{}, core.ErrObjectLimit
	}
	var id uint16
	if len(state.free) != 0 {
		id = state.free[len(state.free)-1]
		state.free = state.free[:len(state.free)-1]
	} else {
		id = state.next
		if id == 0 {
			return core.ObjectRef{}, core.ErrObjectLimit
		}
		state.next++
	}
	generation := state.generations[id] + 1
	if generation == 0 {
		generation = 1
	}
	state.generations[id] = generation
	ref := core.ObjectRef{ID: id, Generation: generation}
	state.objects[subjectID] = ref
	return ref, nil
}

func (state *roomObjectRefs) release(subjectID int64) {
	ref, exists := state.objects[subjectID]
	if !exists {
		return
	}
	delete(state.objects, subjectID)
	state.free = append(state.free, ref.ID)
}

var _ ReliableRoomFrameSink = (*RoomTransportSink)(nil)
var _ RoomSubjectLifecycle = (*RoomTransportSink)(nil)
