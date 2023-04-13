// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package droptable

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventPrepare = "EventPrepare"
	eventFailed  = "EventFailed"
	eventSuccess = "EventSuccess"

	stateBegin   = "StateBegin"
	stateWaiting = "StateWaiting"
	stateFinish  = "StateFinish"
	stateFailed  = "StateFailed"
)

var (
	dropTableEvents = fsm.Events{
		{Name: eventPrepare, Src: []string{stateBegin}, Dst: stateWaiting},
		{Name: eventSuccess, Src: []string{stateWaiting}, Dst: stateFinish},
		{Name: eventFailed, Src: []string{stateWaiting}, Dst: stateFailed},
	}
	dropTableCallbacks = fsm.Callbacks{
		eventPrepare: prepareCallback,
		eventFailed:  failedCallback,
		eventSuccess: successCallback,
	}
)

func prepareCallback(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	table, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster get table")
		return
	}
	if !exists {
		log.Warn("drop non-existing table", zap.String("schema", params.SourceReq.GetSchemaName()), zap.String("table", params.SourceReq.GetName()))
		return
	}

	shardNodesResult, err := params.ClusterMetadata.GetShardNodeByTableIDs([]storage.TableID{table.ID})
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster get shard by table id")
		return
	}

	result, err := params.ClusterMetadata.DropTable(req.ctx, params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "cluster drop table")
		return
	}

	if len(result.ShardVersionUpdate) != 1 {
		procedure.CancelEventWithLog(event, procedure.ErrDropTableResult, fmt.Sprintf("legnth of shardVersionResult is %d", len(result.ShardVersionUpdate)))
		return
	}

	shardNodes, ok := shardNodesResult.ShardNodes[table.ID]
	if !ok {
		procedure.CancelEventWithLog(event, procedure.ErrShardLeaderNotFound, fmt.Sprintf("cluster get shard by table id, table:%v", table))
		return
	}

	// TODO: consider followers
	leader := storage.ShardNode{}
	found := false
	for _, shardNode := range shardNodes {
		if shardNode.ShardRole == storage.ShardRoleLeader {
			found = true
			leader = shardNode
			break
		}
	}

	if !found {
		procedure.CancelEventWithLog(event, procedure.ErrShardLeaderNotFound, "can't find leader")
		return
	}

	tableInfo := metadata.TableInfo{
		ID:         table.ID,
		Name:       table.Name,
		SchemaID:   table.SchemaID,
		SchemaName: params.SourceReq.GetSchemaName(),
	}
	err = params.Dispatch.DropTableOnShard(req.ctx, leader.NodeName, eventdispatch.DropTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: metadata.ShardInfo{
				ID:      result.ShardVersionUpdate[0].ShardID,
				Role:    storage.ShardRoleLeader,
				Version: result.ShardVersionUpdate[0].CurrVersion,
			},
			PrevVersion: result.ShardVersionUpdate[0].PrevVersion,
		},
		TableInfo: tableInfo,
	})
	if err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch drop table on shard")
		return
	}

	req.ret = tableInfo
}

func successCallback(event *fsm.Event) {
	req := event.Args[0].(*callbackRequest)

	if err := req.p.params.OnSucceeded(req.ret); err != nil {
		log.Error("exec success callback failed")
	}
}

func failedCallback(event *fsm.Event) {
	req := event.Args[0].(*callbackRequest)

	if err := req.p.params.OnFailed(event.Err); err != nil {
		log.Error("exec failed callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure

	ret metadata.TableInfo
}

type ProcedureParams struct {
	ID              uint64
	Dispatch        eventdispatch.Dispatch
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot

	SourceReq   *metaservicepb.DropTableRequest
	OnSucceeded func(metadata.TableInfo) error
	OnFailed    func(error) error
}

func NewDropTableProcedure(params ProcedureParams) (procedure.Procedure, error) {
	shardID, err := validateTable(params)
	if err != nil {
		return nil, err
	}

	relatedVersionInfo := buildRelatedVersionInfo(params, shardID)

	fsm := fsm.NewFSM(
		stateBegin,
		dropTableEvents,
		dropTableCallbacks,
	)

	return &Procedure{
		fsm:                fsm,
		shardID:            shardID,
		relatedVersionInfo: relatedVersionInfo,
		params:             params,
		state:              procedure.StateInit,
	}, nil
}

func buildRelatedVersionInfo(params ProcedureParams, shardID storage.ShardID) procedure.RelatedVersionInfo {
	shardWithVersion := make(map[storage.ShardID]uint64, 1)
	for _, shardView := range params.ClusterSnapshot.Topology.ShardViews {
		if shardView.ShardID == shardID {
			shardWithVersion[shardID] = shardView.Version
		}
	}
	return procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}
}

func validateTable(params ProcedureParams) (storage.ShardID, error) {
	table, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		log.Error("get table", zap.Error(err))
		return 0, err
	}
	if !exists {
		log.Error("drop non-existing table", zap.String("schema", params.SourceReq.GetSchemaName()), zap.String("table", params.SourceReq.GetName()))
		return 0, err
	}

	for _, shardView := range params.ClusterSnapshot.Topology.ShardViews {
		for _, tableID := range shardView.TableIDs {
			if table.ID == tableID {
				return shardView.ShardID, nil
			}
		}
	}

	return 0, errors.WithMessagef(metadata.ErrShardNotFound, "The shard corresponding to the table was not found, schema:%s, table:%s", params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
}

type Procedure struct {
	fsm                *fsm.FSM
	shardID            storage.ShardID
	relatedVersionInfo procedure.RelatedVersionInfo
	params             ProcedureParams

	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Typ() procedure.Typ {
	return procedure.DropTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		ctx: ctx,
		p:   p,
	}

	if err := p.fsm.Event(eventPrepare, req); err != nil {
		err1 := p.fsm.Event(eventFailed, req)
		p.updateState(procedure.StateFailed)
		if err1 != nil {
			err = errors.WithMessagef(err, "send eventFailed, err:%v", err1)
		}
		return errors.WithMessage(err, "send eventPrepare")
	}

	if err := p.fsm.Event(eventSuccess, req); err != nil {
		return errors.WithMessage(err, "send eventSuccess")
	}

	p.updateState(procedure.StateFinished)
	return nil
}

func (p *Procedure) Cancel(_ context.Context) error {
	p.updateState(procedure.StateCancelled)
	return nil
}

func (p *Procedure) State() procedure.State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

func (p *Procedure) updateState(state procedure.State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}
