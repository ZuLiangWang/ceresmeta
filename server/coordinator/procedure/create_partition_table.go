// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/looplab/fsm"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	eventAllocShard           = "EventAllocShard"
	eventCreatePartitionTable = "EventCreatePartitionTable"
	eventCreateDataTables     = "EventCreateDataTables"
	eventOpenPartitionTables  = "EventOpenPartitionTables"
	eventFinish               = "EventSuccess"

	stateBegin                = "StateCreatePartitionTableBegin"
	stateAllocShard           = "StateAllocShard"
	stateCreatePartitionTable = "StateCreatePartitionTableCreateSuperTable"
	stateCreateDataTables     = "StateCreatePartitionTableCreateDataTables"
	stateOpenPartitionTables  = "StateCreatePartitionTableOpenSuperTables"
	stateFinish               = "StateCreatePartitionTableFinish"
)

var (
	createPartitionTableEvents = fsm.Events{
		{Name: eventAllocShard, Src: []string{stateBegin}, Dst: stateAllocShard},
		{Name: eventCreatePartitionTable, Src: []string{stateAllocShard}, Dst: stateCreatePartitionTable},
		{Name: eventCreateDataTables, Src: []string{stateCreatePartitionTable}, Dst: stateCreateDataTables},
		{Name: eventOpenPartitionTables, Src: []string{stateCreateDataTables}, Dst: stateOpenPartitionTables},
		{Name: eventFinish, Src: []string{stateOpenPartitionTables}, Dst: stateFinish},
	}
	createPartitionTableCallbacks = fsm.Callbacks{
		eventAllocShard:           allocShardCallback,
		eventCreatePartitionTable: createPartitionTableCallback,
		eventCreateDataTables:     createDataTablesCallback,
		eventOpenPartitionTables:  openPartitionTablesCallback,
		eventFinish:               finishCallback,
	}
)

type CreatePartitionTableProcedure struct {
	id          uint64
	fsm         *fsm.FSM
	cluster     *cluster.Cluster
	dispatch    eventdispatch.Dispatch
	storage     Storage
	shardPicker ShardPicker

	req *metaservicepb.CreateTableRequest

	partitionTableNum uint

	partitionTableShards []cluster.ShardNodeWithVersion
	dataTablesShards     []cluster.ShardNodeWithVersion

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	lock  sync.RWMutex
	state State
}

func NewCreatePartitionTableProcedure(id uint64, cluster *cluster.Cluster, dispatch eventdispatch.Dispatch, storage Storage, picker ShardPicker, req *metaservicepb.CreateTableRequest, partitionTableNum uint, onSuccessed func(cluster.CreateTableResult) error, onFailed func(error) error) *CreatePartitionTableProcedure {
	fsm := fsm.NewFSM(
		stateBegin,
		createPartitionTableEvents,
		createPartitionTableCallbacks,
	)
	return &CreatePartitionTableProcedure{
		id:                id,
		fsm:               fsm,
		cluster:           cluster,
		dispatch:          dispatch,
		storage:           storage,
		shardPicker:       picker,
		req:               req,
		partitionTableNum: partitionTableNum,
		onSucceeded:       onSuccessed,
		onFailed:          onFailed,
	}
}

func (p *CreatePartitionTableProcedure) ID() uint64 {
	return p.id
}

func (p *CreatePartitionTableProcedure) Typ() Typ {
	return CreatePartitionTable
}

func (p *CreatePartitionTableProcedure) Start(ctx context.Context) error {
	p.updateStateWithLock(StateRunning)

	createPartitionTableRequest := &CreatePartitionTableCallbackRequest{
		ctx:               ctx,
		cluster:           p.cluster,
		dispatch:          p.dispatch,
		shardPicker:       p.shardPicker,
		sourceReq:         p.req,
		onSucceeded:       p.onSucceeded,
		onFailed:          p.onFailed,
		partitionTableNum: p.partitionTableNum,
	}

	for {
		switch p.fsm.Current() {
		case stateBegin:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
			}
			if err := p.fsm.Event(eventAllocShard, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "create partition table procedure create new shard metadata")
			}
		case stateAllocShard:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
			}
			if err := p.fsm.Event(eventCreatePartitionTable, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "create partition table procedure create new shard view")
			}
		case stateCreatePartitionTable:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
			}
			if err := p.fsm.Event(eventCreateDataTables, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "create partition table procedure create new shard")
			}
		case stateCreateDataTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
			}
			if err := p.fsm.Event(eventOpenPartitionTables, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "create partition table procedure create shard tables")
			}
		case stateOpenPartitionTables:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
			}
			if err := p.fsm.Event(eventFinish, createPartitionTableRequest); err != nil {
				p.updateStateWithLock(StateFailed)
				return errors.WithMessagef(err, "create partition table procedure delete shard tables")
			}
		case stateFinish:
			if err := p.persist(ctx); err != nil {
				return errors.WithMessage(err, "create partition table procedure persist")
			}
			p.updateStateWithLock(StateFinished)
			return nil
		}
	}
}

func (p *CreatePartitionTableProcedure) Cancel(_ context.Context) error {
	p.updateStateWithLock(StateCancelled)
	return nil
}

func (p *CreatePartitionTableProcedure) State() State {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.state
}

type CreatePartitionTableCallbackRequest struct {
	ctx         context.Context
	cluster     *cluster.Cluster
	dispatch    eventdispatch.Dispatch
	shardPicker ShardPicker

	sourceReq *metaservicepb.CreateTableRequest

	onSucceeded func(cluster.CreateTableResult) error
	onFailed    func(error) error

	// The following variables are populated during execution.
	createTableResult    cluster.CreateTableResult
	partitionTableNum    uint
	partitionTableShards []cluster.ShardNodeWithVersion
	dataTablesShards     []cluster.ShardNodeWithVersion
}

func allocShardCallback(event *fsm.Event) {
	req := event.Args[0].(*CreatePartitionTableCallbackRequest)

	partitionTableShards, err := req.shardPicker.PickShards(req.ctx, req.cluster.Name(), int(req.partitionTableNum))
	if err != nil {
		cancelEventWithLog(event, err, "pick partition table shards", zap.String("clusterName", req.cluster.Name()), zap.Int("partitionTableNum", int(req.partitionTableNum)))
		return
	}
	req.partitionTableShards = partitionTableShards

	dataTableShards, err := req.shardPicker.PickShards(req.ctx, req.cluster.Name(), len(req.sourceReq.PartitionInfo.Names))
	if err != nil {
		cancelEventWithLog(event, err, "pick data table shards", zap.String("clusterName", req.cluster.Name()), zap.Int("dataTableNum", len(req.sourceReq.PartitionInfo.Names)))
		return
	}
	req.dataTablesShards = dataTableShards
}

// 1. Create super table in target node.
func createPartitionTableCallback(event *fsm.Event) {
	req := event.Args[0].(*CreatePartitionTableCallbackRequest)

	// Select first shard to create partition table.
	partitionTableShardNode := req.partitionTableShards[0]

	createTableResult, err := createTableMetadata(req.ctx, req.cluster, req.sourceReq.GetSchemaName(), req.sourceReq.GetName(), partitionTableShardNode.ShardNode.NodeName)
	if err != nil {
		cancelEventWithLog(event, err, "create table metadata")
		return
	}

	if err = createTableOnShard(req.ctx, req.cluster, req.dispatch, partitionTableShardNode.ShardInfo.ID, buildCreateTableRequest(createTableResult, req.sourceReq)); err != nil {
		cancelEventWithLog(event, err, "dispatch create table on shard")
		return
	}
}

// 2. Create data tables in target nodes.
func createDataTablesCallback(event *fsm.Event) {
	req := event.Args[0].(*CreatePartitionTableCallbackRequest)

	for i, dataTableShard := range req.dataTablesShards {
		createTableResult, err := createTableMetadata(req.ctx, req.cluster, req.sourceReq.GetSchemaName(), req.sourceReq.GetPartitionInfo().Names[i], dataTableShard.ShardNode.NodeName)
		if err != nil {
			cancelEventWithLog(event, err, "create table metadata")
			return
		}

		if err = createTableOnShard(req.ctx, req.cluster, req.dispatch, dataTableShard.ShardInfo.ID, buildCreateTableRequest(createTableResult, req.sourceReq)); err != nil {
			cancelEventWithLog(event, err, "dispatch create table on shard")
			return
		}
	}
}

// 3. Open super table in target nodes.
// TODO: Replace open table implementation, avoid reopening shard.
func openPartitionTablesCallback(event *fsm.Event) {
	req := event.Args[0].(*CreatePartitionTableCallbackRequest)

	partitionTable, _, err := req.cluster.GetTable(req.sourceReq.GetSchemaName(), req.sourceReq.GetName())
	if err != nil {
		cancelEventWithLog(event, err, "get table", zap.String("schemaName", req.sourceReq.GetSchemaName()), zap.String("tableName", req.sourceReq.GetName()))
		return
	}

	req.partitionTableShards = append(req.partitionTableShards[:0], req.partitionTableShards[1:]...)
	for _, partitionTableShard := range req.partitionTableShards {
		// Update table shard mapping.
		originShardTables := req.cluster.GetShardTables([]storage.ShardID{partitionTableShard.ShardInfo.ID}, partitionTableShard.ShardNode.NodeName)[partitionTableShard.ShardInfo.ID]
		originShardTables.Shard.Version++
		originShardTables.Tables = append(originShardTables.Tables, cluster.TableInfo{
			ID:         partitionTable.ID,
			Name:       partitionTable.Name,
			SchemaID:   partitionTable.SchemaID,
			SchemaName: req.sourceReq.GetSchemaName(),
		})
		if err := req.cluster.UpdateShardTables(req.ctx, []cluster.ShardTables{originShardTables}); err != nil {
			cancelEventWithLog(event, err, "update shard tables")
			return
		}

		// Reopen data table shard.
		if err := req.dispatch.CloseShard(req.ctx, partitionTableShard.ShardNode.NodeName, eventdispatch.CloseShardRequest{
			ShardID: uint32(partitionTableShard.ShardNode.ID),
		}); err != nil {
			cancelEventWithLog(event, err, "close shard")
			return
		}

		if err := req.dispatch.OpenShard(req.ctx, partitionTableShard.ShardNode.NodeName, eventdispatch.OpenShardRequest{
			Shard: partitionTableShard.ShardInfo,
		}); err != nil {
			cancelEventWithLog(event, err, "open shard")
			return
		}
	}
}

func finishCallback(event *fsm.Event) {
	req := event.Args[0].(*CreatePartitionTableCallbackRequest)
	log.Info("create partition table finish")

	if err := req.onSucceeded(req.createTableResult); err != nil {
		cancelEventWithLog(event, err, "create partition table on succeeded")
		return
	}
}

func (p *CreatePartitionTableProcedure) updateStateWithLock(state State) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.state = state
}

func (p *CreatePartitionTableProcedure) persist(ctx context.Context) error {
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

type CreatePartitionTableRawData struct {
	CreateTableResult    cluster.CreateTableResult
	PartitionTableNum    uint
	PartitionTableShards []cluster.ShardNodeWithVersion
	DataTablesShards     []cluster.ShardNodeWithVersion
}

func (p *CreatePartitionTableProcedure) convertToMeta() (Meta, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// TODO: How to pass, fix
	rawData := CreatePartitionTableRawData{
		PartitionTableNum:    p.partitionTableNum,
		PartitionTableShards: p.partitionTableShards,
		DataTablesShards:     p.dataTablesShards,
	}
	rawDataBytes, err := json.Marshal(rawData)
	if err != nil {
		return Meta{}, ErrEncodeRawData.WithCausef("marshal raw data, procedureID:%v, err:%v", p.id, err)
	}

	meta := Meta{
		ID:    p.id,
		Typ:   Split,
		State: p.state,

		RawData: rawDataBytes,
	}

	return meta, nil
}
