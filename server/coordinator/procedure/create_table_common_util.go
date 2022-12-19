// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"fmt"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

func createTableMetadata(ctx context.Context, c *cluster.Cluster, schemaName string, tableName string, nodeName string) (cluster.CreateTableResult, error) {
	_, exists, err := c.GetTable(schemaName, tableName)
	if err != nil {
		return cluster.CreateTableResult{}, errors.WithMessage(err, "cluster get table")
	}
	if exists {
		return cluster.CreateTableResult{}, errors.WithMessage(ErrTableAlreadyExists, fmt.Sprintf("create an existing table, schemaName:%s, tableName:%s", schemaName, tableName))
	}

	createTableResult, err := c.CreateTable(ctx, nodeName, schemaName, tableName)
	if err != nil {
		return cluster.CreateTableResult{}, errors.WithMessage(err, "create table")
	}
	return createTableResult, nil
}

func createTableOnShard(ctx context.Context, c *cluster.Cluster, dispatch eventdispatch.Dispatch, shardID storage.ShardID, request eventdispatch.CreateTableOnShardRequest) error {
	shardNodes, err := c.GetShardNodesByShardID(shardID)
	if err != nil {
		return errors.WithMessage(err, "cluster get shardNode by id")
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
		return errors.WithMessage(ErrShardLeaderNotFound, fmt.Sprintf("shard node can't find leader, shardID:%d", shardID))
	}

	err = dispatch.CreateTableOnShard(ctx, leader.NodeName, request)
	if err != nil {
		return errors.WithMessage(err, "create table on shard")
	}
	return nil
}

func buildCreateTableRequest(createTableResult cluster.CreateTableResult, req *metaservicepb.CreateTableRequest) eventdispatch.CreateTableOnShardRequest {
	return eventdispatch.CreateTableOnShardRequest{
		UpdateShardInfo: eventdispatch.UpdateShardInfo{
			CurrShardInfo: cluster.ShardInfo{
				ID: createTableResult.ShardVersionUpdate.ShardID,
				// TODO: dispatch CreateTableOnShard to followers?
				Role:    storage.ShardRoleLeader,
				Version: createTableResult.ShardVersionUpdate.CurrVersion,
			},
			PrevVersion: createTableResult.ShardVersionUpdate.PrevVersion,
		},
		TableInfo: cluster.TableInfo{
			ID:         createTableResult.Table.ID,
			Name:       createTableResult.Table.Name,
			SchemaID:   createTableResult.Table.SchemaID,
			SchemaName: req.GetSchemaName(),
		},
		EncodedSchema:    req.EncodedSchema,
		Engine:           req.Engine,
		CreateIfNotExist: req.CreateIfNotExist,
		Options:          req.Options,
	}
}
