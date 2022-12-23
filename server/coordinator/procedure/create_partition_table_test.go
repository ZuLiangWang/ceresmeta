// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"

	"github.com/CeresDB/ceresdbproto/golang/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/stretchr/testify/require"
)

func TestCreatePartitionTable(t *testing.T) {
	re := require.New(t)
	ctx := context.Background()
	dispatch := MockDispatch{}
	manager, c := prepare(t)
	s := NewTestStorage(t)

	shardPicker := NewRandomShardPicker(manager)

	request := &metaservicepb.CreateTableRequest{
		Header: &metaservicepb.RequestHeader{
			Node:        nodeName0,
			ClusterName: clusterName,
		},
		PartitionInfo: &metaservicepb.PartitionInfo{
			Names: []string{"p1", "p2"},
		},
		SchemaName: testSchemaName,
		Name:       testTableName0,
	}

	partitionTableShards, err := shardPicker.PickShards(ctx, c.Name(), defaultPartitionTableRatioOfNodes)
	re.NoError(err)
	dataTableShards, err := shardPicker.PickShards(ctx, c.Name(), len(request.PartitionInfo.Names))
	re.NoError(err)

	procedure := NewCreatePartitionTableProcedure(CreatePartitionTableProcedureRequest{
		1, c, dispatch, s, request, partitionTableShards, dataTableShards, func(_ cluster.CreateTableResult) error {
			return nil
		}, func(_ error) error {
			return nil
		},
	})

	err = procedure.Start(ctx)
	re.NoError(err)
}
