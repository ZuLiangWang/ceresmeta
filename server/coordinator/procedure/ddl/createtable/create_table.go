/*
 * Copyright 2022 The CeresDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package createtable

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/assert"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventCheckTableExists  = "EventCheckTableExists"
	eventCreateTableAssign = "EventCreateTableAssign"
	eventCreateMetadata    = "EventCreateMetadata"
	eventCreateOnShard     = "EventCreateOnShard"
	eventFinish            = "EventFinish"

	stateBegin             = "StateBegin"
	stateCheckTableExists  = "StateCheckTableExists"
	stateCreateTableAssign = "StateCreateTableAssign"
	stateCreateMetadata    = "StateCreateMetadata"
	stateCreateOnShard     = "StateCreateOnShard"
	stateFinish            = "StateFinish"
)

var (
	createTableEvents = fsm.Events{
		{Name: eventCheckTableExists, Src: []string{stateBegin}, Dst: stateCheckTableExists},
		{Name: eventCreateTableAssign, Src: []string{stateCheckTableExists}, Dst: stateCreateTableAssign},
		{Name: eventCreateMetadata, Src: []string{stateCreateTableAssign}, Dst: stateCreateMetadata},
		{Name: eventCreateOnShard, Src: []string{stateCreateMetadata}, Dst: stateCreateOnShard},
		{Name: eventFinish, Src: []string{stateCreateOnShard}, Dst: stateFinish},
	}
	createTableCallbacks = fsm.Callbacks{
		eventCheckTableExists:  checkTableExists,
		eventCreateTableAssign: createTableAssign,
		eventCreateMetadata:    createMetadata,
		eventCreateOnShard:     createOnShard,
		eventFinish:            createFinish,
	}
)

func checkTableExists(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	// Check whether the table metadata already exists.
	table, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get table metadata")
		return
	}
	if !exists {
		return
	}

	// Check whether the table shard mapping already exists.
	_, exists = params.ClusterMetadata.GetTableShard(req.ctx, table)
	if exists {
		procedure.CancelEventWithLog(event, metadata.ErrTableAlreadyExists, "table already exists")
		return
	}
}

func createTableAssign(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	schemaName := params.SourceReq.GetSchemaName()
	tableName := params.SourceReq.GetName()

	targetShardID, exists, err := params.ClusterMetadata.GetAssignTable(req.ctx, schemaName, tableName)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get table assign", zap.String("schemaName", schemaName), zap.String("tableName", tableName))
		return
	}
	if exists {
		if targetShardID != params.ShardID {
			procedure.CancelEventWithLog(event, procedure.ErrShardNotMatch, "target shard not match to persist data", zap.String("schemaName", schemaName), zap.String("tableName", tableName), zap.Uint32("targetShardID", uint32(targetShardID)), zap.Uint32("persistShardID", uint32(params.ShardID)))
			return
		}
		return
	}

	if err := params.ClusterMetadata.AssignTable(req.ctx, schemaName, tableName, params.ShardID); err != nil {
		procedure.CancelEventWithLog(event, err, "persist table assign")
		return
	}

	log.Debug("create table assign finish", zap.String("schemaName", schemaName), zap.String("tableName", tableName))
}

func createMetadata(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	_, exists, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get table metadata")
		return
	}
	if exists {
		log.Info("table metadata already exists", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()))
		return
	}

	createTableMetadataRequest := metadata.CreateTableMetadataRequest{
		SchemaName:    params.SourceReq.GetSchemaName(),
		TableName:     params.SourceReq.GetName(),
		PartitionInfo: storage.PartitionInfo{Info: params.SourceReq.PartitionTableInfo.GetPartitionInfo()},
	}
	_, err = params.ClusterMetadata.CreateTableMetadata(req.ctx, createTableMetadataRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "create table metadata")
		return
	}

	log.Debug("create table metadata finish", zap.String("tableName", createTableMetadataRequest.TableName))
}

func createOnShard(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	table, ok, err := params.ClusterMetadata.GetTable(params.SourceReq.GetSchemaName(), params.SourceReq.GetName())
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get table metadata failed", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()))
		return
	}
	if !ok {
		procedure.CancelEventWithLog(event, err, "table metadata not found", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()))
		return
	}

	shardVersionUpdate := metadata.ShardVersionUpdate{
		ShardID:       params.ShardID,
		LatestVersion: req.p.relatedVersionInfo.ShardWithVersion[params.ShardID],
	}

	createTableRequest := ddl.BuildCreateTableRequest(table, shardVersionUpdate, params.SourceReq)
	latestShardVersion, err := ddl.CreateTableOnShard(req.ctx, params.ClusterMetadata, params.Dispatch, params.ShardID, createTableRequest)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}

	log.Debug("dispatch createTableOnShard finish", zap.String("tableName", table.Name))

	shardVersionUpdate = metadata.ShardVersionUpdate{
		ShardID:       params.ShardID,
		LatestVersion: latestShardVersion,
	}

	err = params.ClusterMetadata.AddTableTopology(req.ctx, shardVersionUpdate, table)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "add table topology")
		return
	}

	req.createTableResult = &metadata.CreateTableResult{
		Table:              table,
		ShardVersionUpdate: shardVersionUpdate,
	}

	log.Debug("add table topology finish", zap.String("tableName", table.Name))
}

func createFinish(event *fsm.Event) {
	req, err := procedure.GetRequestFromEvent[*callbackRequest](event)
	if err != nil {
		procedure.CancelEventWithLog(event, err, "get request from event")
		return
	}
	params := req.p.params

	if err := req.p.params.ClusterMetadata.DeleteAssignTable(req.ctx, params.SourceReq.GetSchemaName(), params.SourceReq.GetName()); err != nil {
		log.Warn("delete assign table failed", zap.String("schemaName", params.SourceReq.GetSchemaName()), zap.String("tableName", params.SourceReq.GetName()))
	}

	assert.Assert(req.createTableResult != nil)
	if err := req.p.params.OnSucceeded(*req.createTableResult); err != nil {
		log.Error("exec success callback failed")
	}
}

// callbackRequest is fsm callbacks param.
type callbackRequest struct {
	ctx context.Context
	p   *Procedure

	createTableResult *metadata.CreateTableResult
}

type ProcedureParams struct {
	Dispatch        eventdispatch.Dispatch
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	ID              uint64
	ShardID         storage.ShardID
	SourceReq       *metaservicepb.CreateTableRequest
	OnSucceeded     func(metadata.CreateTableResult) error
	OnFailed        func(error) error
}

func NewProcedure(params ProcedureParams) (procedure.Procedure, error) {
	fsm := fsm.NewFSM(
		stateBegin,
		createTableEvents,
		createTableCallbacks,
	)

	relatedVersionInfo, err := buildRelatedVersionInfo(params)
	if err != nil {
		return nil, err
	}

	return &Procedure{
		fsm:                fsm,
		params:             params,
		relatedVersionInfo: relatedVersionInfo,
		state:              procedure.StateInit,
		lock:               sync.RWMutex{},
	}, nil
}

type Procedure struct {
	fsm                *fsm.FSM
	params             ProcedureParams
	relatedVersionInfo procedure.RelatedVersionInfo

	// Protect the state.
	lock  sync.RWMutex
	state procedure.State
}

func (p *Procedure) RelatedVersionInfo() procedure.RelatedVersionInfo {
	return p.relatedVersionInfo
}

func buildRelatedVersionInfo(params ProcedureParams) (procedure.RelatedVersionInfo, error) {
	shardWithVersion := make(map[storage.ShardID]uint64, 1)
	shardView, exists := params.ClusterSnapshot.Topology.ShardViewsMapping[params.ShardID]
	if !exists {
		return procedure.RelatedVersionInfo{}, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found in topology, shardID:%d", params.ShardID)
	}
	shardWithVersion[params.ShardID] = shardView.Version
	return procedure.RelatedVersionInfo{
		ClusterID:        params.ClusterSnapshot.Topology.ClusterView.ClusterID,
		ShardWithVersion: shardWithVersion,
		ClusterVersion:   params.ClusterSnapshot.Topology.ClusterView.Version,
	}, nil
}

func (p *Procedure) Priority() procedure.Priority {
	return procedure.PriorityLow
}

func (p *Procedure) ID() uint64 {
	return p.params.ID
}

func (p *Procedure) Kind() procedure.Kind {
	return procedure.CreateTable
}

func (p *Procedure) Start(ctx context.Context) error {
	p.updateState(procedure.StateRunning)

	req := &callbackRequest{
		ctx:               ctx,
		p:                 p,
		createTableResult: nil,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.fsm.Event(eventCheckTableExists, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateCheckTableExists:
			if err := p.fsm.Event(eventCreateTableAssign, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateCreateTableAssign:
			if err := p.fsm.Event(eventCreateMetadata, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateCreateMetadata:
			if err := p.fsm.Event(eventCreateOnShard, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateCreateOnShard:
			if err := p.fsm.Event(eventFinish, req); err != nil {
				_ = p.params.OnFailed(err)
				return err
			}
		case stateFinish:
			p.updateState(procedure.StateFinished)
			return nil
		}
	}
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
