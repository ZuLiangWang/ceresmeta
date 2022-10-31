// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	heartbeatCheckInterval               = 10 * time.Second
	heartbeatKeepAliveIntervalSec uint64 = 15
	metaListBatchSize                    = 100
)

type Scheduler struct {
	// This lock is used to protect the field `running`.
	lock    sync.RWMutex
	running bool

	clusterManager   cluster.Manager
	procedureManager procedure.Manager
	procedureFactory *procedure.Factory
	dispatch         eventdispatch.Dispatch
	storage          procedure.Storage

	checkNodeTicker *time.Ticker
}

func NewScheduler(clusterManager cluster.Manager, procedureManager procedure.Manager, procedureFactory *procedure.Factory, dispatch eventdispatch.Dispatch) *Scheduler {
	return &Scheduler{
		running:          false,
		clusterManager:   clusterManager,
		procedureManager: procedureManager,
		procedureFactory: procedureFactory,
		dispatch:         dispatch,
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.running {
		log.Warn("scheduler has already been started")
		return nil
	}

	s.running = true
	ticker := time.NewTicker(heartbeatCheckInterval)
	s.checkNodeTicker = ticker
	go s.checkNode(ctx, ticker)
	return nil
}

func (s *Scheduler) Stop(_ context.Context) error {
	s.checkNodeTicker.Stop()
	return nil
}

func (s *Scheduler) ProcessHeartbeat(_ context.Context, _ *metaservicepb.NodeInfo) {
	// Check node version and update to latest.
}

func (s *Scheduler) checkNode(ctx context.Context, ticker *time.Ticker) {
	for t := range ticker.C {
		clusters, err := s.clusterManager.ListClusters(ctx)
		if err != nil {
			log.Error("list clusters failed", zap.Error(err))
			continue
		}
		for _, c := range clusters {
			nodes := c.GetRegisteredNodes()
			nodeShards, err := c.GetNodeShards(ctx)
			if err != nil {
				log.Error("get node shards failed", zap.Error(err))
				continue
			}
			nodeShardsMapping := map[string][]*cluster.ShardInfo{}
			for _, nodeShard := range nodeShards.NodeShards {
				_, exists := nodeShardsMapping[nodeShard.Endpoint]
				if !exists {
					nodeShardsMapping[nodeShard.Endpoint] = []*cluster.ShardInfo{}
				}
				nodeShardsMapping[nodeShard.Endpoint] = append(nodeShardsMapping[nodeShard.Endpoint], nodeShard.ShardInfo)
			}
			s.processNodes(ctx, nodes, t, nodeShardsMapping)
		}
	}
}

func (s *Scheduler) processNodes(ctx context.Context, nodes []*cluster.RegisteredNode, t time.Time, nodeShardsMapping map[string][]*cluster.ShardInfo) {
	for _, node := range nodes {
		// Determines whether node is expired.
		if !node.IsExpired(uint64(t.Unix()), heartbeatKeepAliveIntervalSec) {
			// Shard versions of CeresDB and CeresMeta may be inconsistent. And close extra shards and open missing shards if so.
			realShards := node.GetShardInfos()
			expectShards := nodeShardsMapping[node.GetMeta().GetName()]
			err := s.applyMetadataShardInfo(ctx, node.GetMeta().GetName(), realShards, expectShards)
			if err != nil {
				log.Error("apply metadata failed", zap.Error(err))
			}
		}
	}
}

// applyMetadataShardInfo verify shardInfo in heartbeats and metadata, they are forcibly synchronized to the latest version if they are inconsistent.
// TODO: Encapsulate the following logic as a standalone ApplyProcedure.
func (s *Scheduler) applyMetadataShardInfo(ctx context.Context, nodeName string, realShards []*cluster.ShardInfo, expectShards []*cluster.ShardInfo) error {
	realShardInfoMapping := make(map[uint32]*cluster.ShardInfo, len(realShards))
	expectShardInfoMapping := make(map[uint32]*cluster.ShardInfo, len(expectShards))
	for _, realShard := range realShards {
		realShardInfoMapping[realShard.ID] = realShard
	}
	for _, expectShard := range expectShards {
		expectShardInfoMapping[expectShard.ID] = expectShard
	}

	shardsNeedToReopen := []*cluster.ShardInfo{}
	shardsNeedToCloseAndReopen := []*cluster.ShardInfo{}
	shardNeedToClose := []*cluster.ShardInfo{}

	// This includes the following cases:
	for _, expectShard := range expectShards {
		realShard, exists := realShardInfoMapping[expectShard.ID]

		// 1. Shard exists in metadata and not exists in node, reopen lack shards on node.
		if !exists {
			shardsNeedToReopen = append(shardsNeedToReopen, expectShard)
			continue
		}

		// 2. Shard exists in both metadata and node, versions are consistent, do nothing.
		if realShard.Version == expectShard.Version {
			continue
		}
		shardsNeedToCloseAndReopen = append(shardsNeedToCloseAndReopen, expectShard)
	}

	// 4. Shard exists in node and not exists in metadata, close extra shard on node.
	for _, realShard := range realShards {
		_, ok := expectShardInfoMapping[realShard.ID]
		if ok {
			continue
		}
		shardNeedToClose = append(shardNeedToClose, realShard)
	}

	applyProcedure, err := s.procedureFactory.CreateApplyProcedure(ctx, &procedure.ApplyRequest{NodeName: nodeName, ShardsNeedReopen: shardsNeedToReopen, ShardsNeedClose: shardNeedToClose, ShardsNeedCloseAndReopen: shardsNeedToCloseAndReopen})
	if err != nil {
		return errors.WithMessagef(err, "create apply procedure failed, target nodeName:%s", nodeName)
	}
	if err := s.procedureManager.Submit(ctx, applyProcedure); err != nil {
		return errors.WithMessagef(err, "submit apply procedure failed, procedureID:%s, target nodeName:%s", applyProcedure.ID(), nodeName)
	}

	return nil
}

func needRetry(m *procedure.Meta) bool {
	if m.State == procedure.StateCancelled || m.State == procedure.StateFinished {
		return false
	}
	return true
}

func (s *Scheduler) retryAll(ctx context.Context) error {
	metas, err := s.storage.List(ctx, metaListBatchSize)
	if err != nil {
		return errors.WithMessage(err, "storage list meta failed")
	}
	for _, meta := range metas {
		if !needRetry(meta) {
			continue
		}
		p := restoreProcedure(meta)
		err := s.retry(ctx, p)
		return errors.WithMessagef(err, "retry procedure failed, procedureID:%d", p.ID())
	}
	return nil
}

func (s *Scheduler) retry(ctx context.Context, procedure procedure.Procedure) error {
	err := s.procedureManager.Submit(ctx, procedure)
	if err != nil {
		return errors.WithMessagef(err, "start procedure failed, procedureID:%d", procedure.ID())
	}
	return nil
}

// Load meta and restore procedure.
func restoreProcedure(meta *procedure.Meta) procedure.Procedure {
	switch meta.Typ {
	case procedure.Create:
		return nil
	case procedure.Delete:
		return nil
	case procedure.TransferLeader:
		return nil
	case procedure.Migrate:
		return nil
	case procedure.Split:
		return nil
	case procedure.Merge:
		return nil
	case procedure.Scatter:
		return nil
	}
	return nil
}
