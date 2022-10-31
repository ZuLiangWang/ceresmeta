// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/looplab/fsm"
	"sync"
)

const (
	eventApplyPrepare = "EventApplyPrepare"
	eventApplyFailed  = "EventApplyFailed"
	eventApplySuccess = "EventApplySuccess"

	stateApplyBegin   = "StateApplyBegin"
	stateApplyWaiting = "StateApplyWaiting"
	stateApplyFinish  = "StateApplyFinish"
	stateApplyFailed  = "StateApplyFailed"
)

var (
	scatterEvents = fsm.Events{
		{Name: eventScatterPrepare, Src: []string{stateScatterBegin}, Dst: stateScatterWaiting},
		{Name: eventScatterSuccess, Src: []string{stateScatterWaiting}, Dst: stateScatterFinish},
		{Name: eventScatterFailed, Src: []string{stateScatterWaiting}, Dst: stateScatterFailed},
	}
	scatterCallbacks = fsm.Callbacks{
		eventScatterPrepare: scatterPrepareCallback,
		eventScatterFailed:  scatterFailedCallback,
		eventScatterSuccess: scatterSuccessCallback,
	}
)

// ApplyProcedure used for apply the latest cluster topology to CeresDB node when metadata version is not equal to CeresDB node version.
type ApplyProcedure struct {
	id       uint64
	fsm      *fsm.FSM
	dispatch eventdispatch.Dispatch
	cluster  *cluster.Cluster

	lock  sync.RWMutex
	state State

	nodeName                   string
	shardsNeedToReopen         []*cluster.ShardInfo
	shardsNeedToCloseAndReopen []*cluster.ShardInfo
	shardsNeedToClose          []*cluster.ShardInfo
}

func NewApplyProcedure(dispatch eventdispatch.Dispatch, id uint64, nodeName string, shardsNeedToReopen []*cluster.ShardInfo, ShardsNeedToCloseAndReopen []*cluster.ShardInfo, shardNeedToClose []*cluster.ShardInfo) Procedure {
	return &ApplyProcedure{
		id: id,
		// fsm:fsm.NewFSM("",),
		nodeName:                   nodeName,
		shardsNeedToReopen:         shardsNeedToReopen,
		shardsNeedToClose:          shardNeedToClose,
		shardsNeedToCloseAndReopen: ShardsNeedToCloseAndReopen,
		dispatch:                   dispatch,
	}
}

func (p *ApplyProcedure) ID() uint64 {
	return p.id
}

func (p *ApplyProcedure) Typ() Typ {
	return Apply
}

func (p *ApplyProcedure) Start(ctx context.Context) error {

	// // Reopen shards.
	// if err := s.dispatch.OpenShard(ctx, node, &eventdispatch.OpenShardRequest{Shard: expectShard}); err != nil {
	// 	return errors.WithMessagef(err, "reopen shard failed, shardInfo:%d", expectShard.ID)
	// }
	//
	// // Close and reopen shards.
	// if err := s.dispatch.CloseShard(ctx, node, &eventdispatch.CloseShardRequest{ShardID: expectShard.ID}); err != nil {
	// 	return errors.WithMessagef(err, "close shard failed, shardInfo:%d", expectShard.ID)
	// }
	// if err := s.dispatch.OpenShard(ctx, node, &eventdispatch.OpenShardRequest{Shard: expectShard}); err != nil {
	// 	return errors.WithMessagef(err, "reopen shard failed, shardInfo:%d", expectShard.ID)
	// }
	//
	// // Close shards.
	// if err := s.dispatch.CloseShard(ctx, node, &eventdispatch.CloseShardRequest{ShardID: realShard.ID}); err != nil {
	// 	return errors.WithMessagef(err, "close shard failed, shardInfo:%d", realShard.ID)
	// }

	return nil
}

func (p *ApplyProcedure) Cancel(ctx context.Context) error {
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
