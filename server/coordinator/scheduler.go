// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/clusterpb"
	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	heartbeatCheckInterval     = 10 * time.Second
	heartbeatKeepAliveInterval = 15 * time.Second
)

type Scheduler struct {
	// This lock is used to protect the field `running`.
	lock    sync.RWMutex
	running bool

	clusterManager   cluster.Manager
	procedureManager procedure.Manager
	procedureFactory *procedure.Factory
	dispatch         eventdispatch.Dispatch

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
		log.Info("procedure scheduler checkNodeState")
		clusters, err := s.clusterManager.ListClusters(ctx)
		if err != nil {
			log.Error("list clusters failed", zap.Error(err))
			continue
		}
		for _, c := range clusters {
			nodes := c.GetNodes()
			nodeShards, err := c.GetNodeShards(ctx)
			if err != nil {
				log.Error("get node shards failed")
				continue
			}
			nodeShardsMapping := make(map[string][]*cluster.ShardInfo)
			for _, nodeShard := range nodeShards.NodeShards {
				_, exists := nodeShardsMapping[nodeShard.Endpoint]
				if !exists {
					nodeShardsMapping[nodeShard.Endpoint] = make([]*cluster.ShardInfo, 0)
				}
				nodeShardsMapping[nodeShard.Endpoint] = append(nodeShardsMapping[nodeShard.Endpoint], nodeShard.ShardInfo)
			}
			for _, node := range nodes {
				if node.GetMeta().GetState() == clusterpb.NodeState_OFFLINE {
					continue
				}
				// Determines whether node is online by compares heartbeatKeepAliveInterval with time.now() - lastTouchTime.
				if uint64(t.Unix())-node.GetMeta().LastTouchTime >= uint64((heartbeatKeepAliveInterval).Seconds()) {
					node.GetMeta().State = clusterpb.NodeState_OFFLINE
				} else {
					// Shard versions of CeresDB and CeresMeta are inconsistent.
					// Close redundant shards and open lack shards.
					heartbeatNodeShardInfos := node.GetShardInfos()
					metadataNodeShardInfos := nodeShardsMapping[node.GetMeta().GetName()]
					err := s.applyMetadataShardInfo(ctx, node.GetMeta().GetName(), heartbeatNodeShardInfos, metadataNodeShardInfos)
					if err != nil {
						log.Error("apply metadata failed", zap.Error(err))
					}
				}
			}
		}
		log.Info("procedure scheduler checkNodeState finish")
	}
}

// applyMetadata verify shardInfo in heartbeats and metadata, they are forcibly synchronized to the latest version if they are inconsistent.
// TODO: Encapsulate the following logic as a standalone ApplyProcedure
func (s *Scheduler) applyMetadataShardInfo(ctx context.Context, node string, heartbeatNodeShardInfos []*cluster.ShardInfo, metadataNodeShardInfos []*cluster.ShardInfo) error {
	heartbeatShardInfoMapping := make(map[uint32]*cluster.ShardInfo)
	for _, heartbeatShardInfo := range heartbeatNodeShardInfos {
		heartbeatShardInfoMapping[heartbeatShardInfo.ID] = heartbeatShardInfo
	}

	for _, metadataShardInfo := range metadataNodeShardInfos {
		heartbeatShardInfo, exists := heartbeatShardInfoMapping[metadataShardInfo.ID]
		if !exists {
			if err := s.dispatch.OpenShard(ctx, node, &eventdispatch.OpenShardRequest{Shard: metadataShardInfo}); err != nil {
				return errors.WithMessage(err, fmt.Sprintf("reopen shard failed, shardInfo:%d", metadataShardInfo.ID))
			}
		} else {
			if heartbeatShardInfo.Version != metadataShardInfo.Version {
				if err := s.dispatch.CloseShard(ctx, node, &eventdispatch.CloseShardRequest{ShardID: metadataShardInfo.ID}); err != nil {
					return errors.WithMessage(err, fmt.Sprintf("close shard failed, shardInfo:%d", metadataShardInfo.ID))
				}
				if err := s.dispatch.OpenShard(ctx, node, &eventdispatch.OpenShardRequest{Shard: metadataShardInfo}); err != nil {
					return errors.WithMessage(err, fmt.Sprintf("reopen shard failed, shardInfo:%d", metadataShardInfo.ID))
				}
			}
		}
	}
	return nil
}
