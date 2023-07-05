// Copyright 2022-2023 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/cluster/metadata"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultTimeout           = time.Second * 20
	cluster1                 = "ceresdbCluster1"
	defaultSchema            = "ceresdbSchema"
	defaultNodeCount         = 2
	defaultReplicationFactor = 1
	defaultShardTotal        = 8
	defaultTopologyType      = storage.TopologyTypeStatic
	node1                    = "127.0.0.1:8081"
	node2                    = "127.0.0.2:8081"
	defaultSchemaID          = 0
	testRootPath             = "/rootPath"
	defaultIDAllocatorStep   = 20
)

func newTestStorage(t *testing.T) (storage.Storage, clientv3.KV, *clientv3.Client, etcdutil.CloseFn) {
	_, client, closeSrv := etcdutil.PrepareEtcdServerAndClient(t)
	storage := storage.NewStorageWithEtcdBackend(client, testRootPath, storage.Options{
		MaxScanLimit: 100, MinScanLimit: 10,
	})
	return storage, client, client, closeSrv
}

func newClusterManagerWithStorage(storage storage.Storage, kv clientv3.KV, client *clientv3.Client) (cluster.Manager, error) {
	return cluster.NewManagerImpl(storage, kv, client, testRootPath, defaultIDAllocatorStep, defaultTopologyType)
}

func TestClusterManager(t *testing.T) {
	re := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	s, kv, client, closeSrv := newTestStorage(t)
	defer closeSrv()
	manager, err := newClusterManagerWithStorage(s, kv, client)
	re.NoError(err)

	re.NoError(manager.Start(ctx))

	testCreateCluster(ctx, re, manager, cluster1)

	testRegisterNode(ctx, re, manager, cluster1, node1)
	testRegisterNode(ctx, re, manager, cluster1, node2)

	testInitShardView(ctx, re, manager, cluster1)

	testGetNodeAndShard(ctx, re, manager, cluster1)

	testGetTables(re, manager, node1, cluster1, 0)

	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)
	testAllocSchemaID(ctx, re, manager, cluster1, defaultSchema, defaultSchemaID)

	var testTableNames []string
	for i := uint64(0); i < 5; i++ {
		testTableName := fmt.Sprintf("testTable%d", i)
		testTableNames = append(testTableNames, testTableName)
		testCreateTable(ctx, re, manager, cluster1, defaultSchema, testTableName, storage.ShardID(i))
	}

	testRouteTables(ctx, re, manager, cluster1, defaultSchema, testTableNames)

	for _, tableName := range testTableNames {
		testDropTable(ctx, re, manager, cluster1, defaultSchema, tableName)
	}

	re.NoError(manager.Stop(ctx))
}

func testGetNodeAndShard(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string) {
	c, err := manager.GetCluster(ctx, clusterName)
	re.NoError(err)

	nodes, err := manager.ListRegisterNodes(ctx, cluster1)
	re.NoError(err)
	re.Equal(2, len(nodes))

	node, err := manager.GetRegisteredNode(ctx, cluster1, node1)
	re.NoError(err)
	re.Equal(node1, node.Node.Name)

	nodShards, err := manager.GetNodeShards(ctx, cluster1)
	re.NoError(err)
	re.Equal(int(c.GetMetadata().GetTotalShardNum()), len(nodShards.NodeShards))
}

func testInitShardView(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string) {
	c, err := manager.GetCluster(ctx, clusterName)
	re.NoError(err)
	snapshot := c.GetMetadata().GetClusterSnapshot()
	shardNodes := make([]storage.ShardNode, 0, c.GetMetadata().GetTotalShardNum())
	for _, shardView := range snapshot.Topology.ShardViewsMapping {
		selectNodeIdx, err := rand.Int(rand.Reader, big.NewInt(int64(len(snapshot.RegisteredNodes))))
		re.NoError(err)
		shardNodes = append(shardNodes, storage.ShardNode{
			ID:        shardView.ShardID,
			ShardRole: storage.ShardRoleLeader,
			NodeName:  snapshot.RegisteredNodes[selectNodeIdx.Int64()].Node.Name,
		})
	}
	err = c.GetMetadata().UpdateClusterView(ctx, storage.ClusterStateStable, shardNodes)
	re.NoError(err)
}

func testCreateCluster(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string) {
	_, err := manager.CreateCluster(ctx, clusterName, metadata.CreateClusterOpts{
		NodeCount:         defaultNodeCount,
		ReplicationFactor: defaultReplicationFactor,
		ShardTotal:        defaultShardTotal,
		TopologyType:      defaultTopologyType,
	})
	re.NoError(err)
}

func testRegisterNode(ctx context.Context, re *require.Assertions, manager cluster.Manager,
	clusterName, nodeName string,
) {
	err := manager.RegisterNode(ctx, clusterName, metadata.RegisteredNode{
		Node: storage.Node{
			Name:          nodeName,
			LastTouchTime: uint64(time.Now().UnixMilli()),
			State:         storage.NodeStateOnline,
		}, ShardInfos: []metadata.ShardInfo{},
	})
	re.NoError(err)
}

func testAllocSchemaID(ctx context.Context, re *require.Assertions, manager cluster.Manager,
	cluster, schema string, schemaID uint32,
) {
	id, _, err := manager.AllocSchemaID(ctx, cluster, schema)
	re.NoError(err)
	re.Equal(storage.SchemaID(schemaID), id)
}

func testCreateTable(ctx context.Context, re *require.Assertions, manager cluster.Manager,
	clusterName, schema, tableName string, shardID storage.ShardID,
) {
	c, err := manager.GetCluster(ctx, clusterName)
	re.NoError(err)
	_, err = c.GetMetadata().CreateTable(ctx, metadata.CreateTableRequest{
		ShardID:       shardID,
		SchemaName:    schema,
		TableName:     tableName,
		PartitionInfo: storage.PartitionInfo{},
	})
	re.NoError(err)
}

func testGetTables(re *require.Assertions, manager cluster.Manager, node, cluster string, num int) {
	shardIDs := make([]storage.ShardID, 0, defaultShardTotal)
	for i := 0; i < defaultShardTotal; i++ {
		shardIDs = append(shardIDs, storage.ShardID(i))
	}
	shardTables, err := manager.GetTables(cluster, node, shardIDs)
	re.NoError(err)
	re.Equal(defaultShardTotal, len(shardTables))

	tableNum := 0
	for _, tables := range shardTables {
		re.Equal(storage.ShardRoleLeader, tables.Shard.Role)
		tableNum += len(tables.Tables)
	}
	re.Equal(num, tableNum)
}

func testRouteTables(ctx context.Context, re *require.Assertions, manager cluster.Manager, cluster, schema string, tableNames []string) {
	ret, err := manager.RouteTables(ctx, cluster, schema, tableNames)
	re.NoError(err)
	re.Equal(len(tableNames), len(ret.RouteEntries))
	for _, entry := range ret.RouteEntries {
		re.Equal(1, len(entry.NodeShards))
		re.Equal(storage.ShardRoleLeader, entry.NodeShards[0].ShardNode.ShardRole)
	}
}

func testDropTable(ctx context.Context, re *require.Assertions, manager cluster.Manager, clusterName string, schemaName string, tableName string) {
	err := manager.DropTable(ctx, clusterName, schemaName, tableName)
	re.NoError(err)
}
