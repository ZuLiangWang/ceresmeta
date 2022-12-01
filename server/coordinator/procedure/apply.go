// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventApplyCloseShard = "EventApplyCloseShard"
	eventApplyOpenShard  = "EventApplyOpenShard"
	eventApplyFinish     = "EventApplyFinish"

	stateApplyBegin      = "StateApplyBegin"
	stateApplyCloseShard = "StateApplyCloseShard"
	stateApplyOpenShard  = "StateApplyOpenShard"
	stateApplyFinish     = "stateApplyFinish"
)

var (
	applyEvents = fsm.Events{
		{Name: eventApplyCloseShard, Src: []string{stateApplyBegin}, Dst: stateApplyCloseShard},
		{Name: eventApplyOpenShard, Src: []string{stateApplyCloseShard}, Dst: stateApplyOpenShard},
		{Name: eventApplyFinish, Src: []string{stateApplyOpenShard}, Dst: stateApplyFinish},
	}
	applyCallbacks = fsm.Callbacks{
		eventApplyCloseShard: applyCloseShardCallback,
		eventApplyOpenShard:  applyOpenShardCallback,
		eventApplyFinish:     applyFinishCallback,
	}
)

type ApplyCallbackRequest struct {
	ctx context.Context

	nodeName                   string
	shardsNeedToReopen         []cluster.ShardInfo
	shardsNeedToCloseAndReopen []cluster.ShardInfo
	shardsNeedToClose          []cluster.ShardInfo

	dispatch eventdispatch.Dispatch
}

// ApplyProcedure used for apply the latest cluster topology to CeresDB node when metadata version is not equal to CeresDB node version.
type ApplyProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch
	storage  Storage

	lock  sync.RWMutex
	state State

	nodeName                   string
	shardsNeedToReopen         []cluster.ShardInfo
	shardsNeedToCloseAndReopen []cluster.ShardInfo
	shardsNeedToClose          []cluster.ShardInfo
}

func NewApplyProcedure(dispatch eventdispatch.Dispatch, id uint64, nodeName string, shardsNeedToReopen []cluster.ShardInfo, shardsNeedToCloseAndReopen []cluster.ShardInfo, shardNeedToClose []cluster.ShardInfo, storage Storage) Procedure {
	return &ApplyProcedure{
		id:                         id,
		fsm:                        fsm.NewFSM(stateApplyBegin, applyEvents, applyCallbacks),
		nodeName:                   nodeName,
		shardsNeedToReopen:         shardsNeedToReopen,
		shardsNeedToClose:          shardNeedToClose,
		shardsNeedToCloseAndReopen: shardsNeedToCloseAndReopen,
		dispatch:                   dispatch,
		storage:                    storage,
	}
}

func (p *ApplyProcedure) ID() uint64 {
	return p.id
}

func (p *ApplyProcedure) Typ() Typ {
	return Apply
}

func (p *ApplyProcedure) Start(ctx context.Context) error {
	log.Info("apply procedure start", zap.Uint64("procedureID", p.id))

	p.updateStateWithLock(StateRunning)

	applyCallbackRequest := &ApplyCallbackRequest{
		ctx:                        ctx,
		nodeName:                   p.nodeName,
		shardsNeedToReopen:         p.shardsNeedToReopen,
		shardsNeedToCloseAndReopen: p.shardsNeedToCloseAndReopen,
		shardsNeedToClose:          p.shardsNeedToClose,
		dispatch:                   p.dispatch,
	}

	for {
		switch p.fsm.Current() {
		case stateApplyBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			if err := p.fsm.Event(eventApplyCloseShard, applyCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "apply procedure close shard")
			}
		case stateApplyCloseShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			if err := p.fsm.Event(eventApplyOpenShard, applyCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "apply procedure open shard")
			}
		case stateApplyOpenShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			if err := p.fsm.Event(eventApplyFinish, applyCallbackRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "apply procedure oepn new leader")
			}
		case stateApplyFinish:
			p.updateStateWithLock(StateFinished)
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "apply procedure persist")
			}
			return nil
		}
	}
}

func (p *ApplyProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *ApplyProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *ApplyProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	p.state = state
	p.lock.Unlock()
}

func applyCloseShardCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ApplyCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	ctx := request.ctx
	dispatch := request.dispatch

	for _, shardInfo := range request.shardsNeedToClose {
		if err := dispatch.CloseShard(ctx, request.nodeName, eventdispatch.CloseShardRequest{ShardID: uint32(shardInfo.ID)}); err != nil {
			cancelEventWithLog(event, err, "close shard failed, shardInfo", zap.Uint32("shardID", uint32(shardInfo.ID)))
			return
		}
	}

	for _, shardInfo := range request.shardsNeedToCloseAndReopen {
		if err := dispatch.CloseShard(ctx, request.nodeName, eventdispatch.CloseShardRequest{ShardID: uint32(shardInfo.ID)}); err != nil {
			cancelEventWithLog(event, err, "close shard failed, shardInfo", zap.Uint32("shardID", uint32(shardInfo.ID)))
			return
		}
	}
}

func applyOpenShardCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ApplyCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	ctx := request.ctx
	dispatch := request.dispatch

	for _, shardInfo := range request.shardsNeedToReopen {
		if err := dispatch.OpenShard(ctx, request.nodeName, eventdispatch.OpenShardRequest{Shard: shardInfo}); err != nil {
			cancelEventWithLog(event, err, "reopen shard failed, shardInfo", zap.Uint32("shardID", uint32(shardInfo.ID)))
			return
		}
	}

	for _, shardInfo := range request.shardsNeedToCloseAndReopen {
		if err := dispatch.OpenShard(ctx, request.nodeName, eventdispatch.OpenShardRequest{Shard: shardInfo}); err != nil {
			cancelEventWithLog(event, err, "reopen shard failed, shardInfo", zap.Uint32("shardID", uint32(shardInfo.ID)))
			return
		}
	}
}

func applyFinishCallback(event *fsm.Event) {
	request, err := getRequestFromEvent[*ApplyCallbackRequest](event)
	if err != nil {
		cancelEventWithLog(event, err, "get request from event")
		return
	}

	log.Info("apply procedure finish", zap.String("targetNode", request.nodeName))
}

func (p *ApplyProcedure) persist(ctx context.Context) error {
	meta, err := p.convertToMeta()
	if err != nil {
		return errors.WithMessage(err, "convert to meta")
	}
	err = p.storage.CreateOrUpdate(ctx, meta)
	if err != nil {
		return errors.WithMessage(err, "createOrUpdate procedure storage")
	}
	return nil
}

type ApplyProcedurePersistRawData struct {
	ID       uint64
	FsmState string
	State    State

	NodeName                   string
	ShardsNeedToReopen         []cluster.ShardInfo
	ShardsNeedToCloseAndReopen []cluster.ShardInfo
	ShardsNeedToClose          []cluster.ShardInfo
}

func (p *ApplyProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	rawData := ApplyProcedurePersistRawData{
		ID:                         p.id,
		FsmState:                   p.fsm.Current(),
		State:                      p.state,
		NodeName:                   p.nodeName,
		ShardsNeedToReopen:         p.shardsNeedToReopen,
		ShardsNeedToCloseAndReopen: p.shardsNeedToCloseAndReopen,
		ShardsNeedToClose:          p.shardsNeedToClose,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.id, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   Apply,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
