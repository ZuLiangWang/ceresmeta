// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import "github.com/CeresDB/ceresdbproto/pkg/clusterpb"

type Node struct {
	meta       *clusterpb.Node
	shardInfos []*ShardInfo
}

func (n Node) GetMeta() *clusterpb.Node {
	return n.meta
}

func (n Node) GetShardInfos() []*ShardInfo {
	shardInfos := make([]*ShardInfo, 0, len(n.shardInfos))
	for _, shardInfo := range shardInfos {
		shardInfos = append(shardInfos, &ShardInfo{
			ID:      shardInfo.ID,
			Role:    shardInfo.Role,
			Version: shardInfo.Version,
		})
	}
	return shardInfos
}

func (n Node) IsAvailable() bool {
	return n.meta.State == clusterpb.NodeState_ONLINE
}
