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

package coordinator

import (
	"context"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/createpartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/createtable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/droppartitiontable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/ddl/droptable"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/split"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure/operation/transferleader"
	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Factory struct {
	logger      *zap.Logger
	idAllocator id.Allocator
	dispatch    eventdispatch.Dispatch
	storage     procedure.Storage
	shardPicker ShardPicker
}

type CreateTableRequest struct {
	ClusterMetadata *metadata.ClusterMetadata
	SourceReq       *metaservicepb.CreateTableRequest

	OnSucceeded func(metadata.CreateTableResult) error
	OnFailed    func(error) error
}

func (request *CreateTableRequest) isPartitionTable() bool {
	return request.SourceReq.PartitionTableInfo != nil
}

type DropTableRequest struct {
	ClusterMetadata *metadata.ClusterMetadata
	ClusterSnapshot metadata.Snapshot
	SourceReq       *metaservicepb.DropTableRequest

	OnSucceeded func(metadata.TableInfo) error
	OnFailed    func(error) error
}

func (d DropTableRequest) IsPartitionTable() bool {
	return d.SourceReq.PartitionTableInfo != nil
}

type TransferLeaderRequest struct {
	Snapshot          metadata.Snapshot
	ShardID           storage.ShardID
	OldLeaderNodeName string
	NewLeaderNodeName string
}

type SplitRequest struct {
	ClusterMetadata *metadata.ClusterMetadata
	SchemaName      string
	TableNames      []string
	Snapshot        metadata.Snapshot
	ShardID         storage.ShardID
	NewShardID      storage.ShardID
	TargetNodeName  string
}

type CreatePartitionTableRequest struct {
	ClusterMetadata *metadata.ClusterMetadata
	SourceReq       *metaservicepb.CreateTableRequest

	OnSucceeded func(metadata.CreateTableResult) error
	OnFailed    func(error) error
}

type BatchRequest struct {
	Batch     []procedure.Procedure
	BatchType procedure.Kind
}

func NewFactory(logger *zap.Logger, allocator id.Allocator, dispatch eventdispatch.Dispatch, storage procedure.Storage) *Factory {
	return &Factory{
		idAllocator: allocator,
		dispatch:    dispatch,
		storage:     storage,
		logger:      logger,
		shardPicker: NewLeastTableShardPicker(),
	}
}

func (f *Factory) MakeCreateTableProcedure(ctx context.Context, request CreateTableRequest) (procedure.Procedure, error) {
	isPartitionTable := request.isPartitionTable()

	if isPartitionTable {
		req := CreatePartitionTableRequest(request)
		return f.makeCreatePartitionTableProcedure(ctx, req)
	}

	return f.makeCreateTableProcedure(ctx, request)
}

func (f *Factory) makeCreateTableProcedure(ctx context.Context, request CreateTableRequest) (procedure.Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}
	snapshot := request.ClusterMetadata.GetClusterSnapshot()

	var targetShardID storage.ShardID
	shardID, exists, err := request.ClusterMetadata.GetAssignTable(ctx, request.SourceReq.SchemaName, request.SourceReq.Name)
	if err != nil {
		return nil, err
	}
	if exists {
		targetShardID = shardID
	} else {
		shards, err := f.shardPicker.PickShards(ctx, snapshot, 1)
		if err != nil {
			f.logger.Error("pick table shard", zap.Error(err))
			return nil, errors.WithMessage(err, "pick table shard")
		}
		if len(shards) != 1 {
			f.logger.Error("pick table shards length not equal 1", zap.Int("shards", len(shards)))
			return nil, errors.WithMessagef(procedure.ErrPickShard, "pick table shard, shards length:%d", len(shards))
		}
		targetShardID = shards[0].ID
	}

	return createtable.NewProcedure(createtable.ProcedureParams{
		Dispatch:        f.dispatch,
		ClusterMetadata: request.ClusterMetadata,
		ClusterSnapshot: snapshot,
		ID:              id,
		ShardID:         targetShardID,
		SourceReq:       request.SourceReq,
		OnSucceeded:     request.OnSucceeded,
		OnFailed:        request.OnFailed,
	})
}

func (f *Factory) makeCreatePartitionTableProcedure(ctx context.Context, request CreatePartitionTableRequest) (procedure.Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	snapshot := request.ClusterMetadata.GetClusterSnapshot()

	nodeNames := make(map[string]int, len(snapshot.Topology.ClusterView.ShardNodes))
	for _, shardNode := range snapshot.Topology.ClusterView.ShardNodes {
		nodeNames[shardNode.NodeName] = 1
	}

	subTableShards, err := f.shardPicker.PickShards(ctx, snapshot, len(request.SourceReq.PartitionTableInfo.SubTableNames))
	if err != nil {
		return nil, errors.WithMessage(err, "pick sub table shards")
	}

	shardNodesWithVersion := make([]metadata.ShardNodeWithVersion, 0, len(subTableShards))
	for _, subTableShard := range subTableShards {
		shardView, exists := snapshot.Topology.ShardViewsMapping[subTableShard.ID]
		if !exists {
			return nil, errors.WithMessagef(metadata.ErrShardNotFound, "shard not found, shardID:%d", subTableShard.ID)
		}
		shardNodesWithVersion = append(shardNodesWithVersion, metadata.ShardNodeWithVersion{
			ShardInfo: metadata.ShardInfo{
				ID:      shardView.ShardID,
				Role:    subTableShard.ShardRole,
				Version: shardView.Version,
				Status:  storage.ShardStatusUnknown,
			},
			ShardNode: subTableShard,
		})
	}

	return createpartitiontable.NewProcedure(createpartitiontable.ProcedureParams{
		ID:              id,
		ClusterMetadata: request.ClusterMetadata,
		ClusterSnapshot: snapshot,
		Dispatch:        f.dispatch,
		Storage:         f.storage,
		SourceReq:       request.SourceReq,
		SubTablesShards: shardNodesWithVersion,
		OnSucceeded:     request.OnSucceeded,
		OnFailed:        request.OnFailed,
	})
}

// CreateDropTableProcedure creates a procedure to do drop table.
//
// And if no error is thrown, the returned boolean value is used to tell whether the procedure is created.
// In some cases, e.g. the table doesn't exist, it should not be an error and false will be returned.
func (f *Factory) CreateDropTableProcedure(ctx context.Context, request DropTableRequest) (procedure.Procedure, bool, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, false, err
	}

	snapshot := request.ClusterMetadata.GetClusterSnapshot()

	if request.IsPartitionTable() {
		return droppartitiontable.NewProcedure(droppartitiontable.ProcedureParams{
			ID:              id,
			ClusterMetadata: request.ClusterMetadata,
			ClusterSnapshot: request.ClusterSnapshot,
			Dispatch:        f.dispatch,
			Storage:         f.storage,
			SourceReq:       request.SourceReq,
			OnSucceeded:     request.OnSucceeded,
			OnFailed:        request.OnFailed,
		})
	}

	return droptable.NewDropTableProcedure(droptable.ProcedureParams{
		ID:              id,
		Dispatch:        f.dispatch,
		ClusterMetadata: request.ClusterMetadata,
		ClusterSnapshot: snapshot,
		SourceReq:       request.SourceReq,
		OnSucceeded:     request.OnSucceeded,
		OnFailed:        request.OnFailed,
	})
}

func (f *Factory) CreateTransferLeaderProcedure(ctx context.Context, request TransferLeaderRequest) (procedure.Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	return transferleader.NewProcedure(transferleader.ProcedureParams{
		ID:                id,
		Dispatch:          f.dispatch,
		Storage:           f.storage,
		ClusterSnapshot:   request.Snapshot,
		ShardID:           request.ShardID,
		OldLeaderNodeName: request.OldLeaderNodeName,
		NewLeaderNodeName: request.NewLeaderNodeName,
	})
}

func (f *Factory) CreateSplitProcedure(ctx context.Context, request SplitRequest) (procedure.Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	return split.NewProcedure(
		split.ProcedureParams{
			ID:              id,
			Dispatch:        f.dispatch,
			Storage:         f.storage,
			ClusterMetadata: request.ClusterMetadata,
			ClusterSnapshot: request.Snapshot,
			ShardID:         request.ShardID,
			NewShardID:      request.NewShardID,
			SchemaName:      request.SchemaName,
			TableNames:      request.TableNames,
			TargetNodeName:  request.TargetNodeName,
		},
	)
}

func (f *Factory) CreateBatchTransferLeaderProcedure(ctx context.Context, request BatchRequest) (procedure.Procedure, error) {
	id, err := f.allocProcedureID(ctx)
	if err != nil {
		return nil, err
	}

	return transferleader.NewBatchTransferLeaderProcedure(id, request.Batch)
}

func (f *Factory) allocProcedureID(ctx context.Context) (uint64, error) {
	id, err := f.idAllocator.Alloc(ctx)
	if err != nil {
		return 0, errors.WithMessage(err, "alloc procedure id")
	}
	return id, nil
}
