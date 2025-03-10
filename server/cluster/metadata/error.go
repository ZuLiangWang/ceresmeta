// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package metadata

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrCreateCluster        = coderr.NewCodeError(coderr.BadRequest, "create cluster")
	ErrUpdateCluster        = coderr.NewCodeError(coderr.Internal, "update cluster")
	ErrStartCluster         = coderr.NewCodeError(coderr.Internal, "start cluster")
	ErrClusterAlreadyExists = coderr.NewCodeError(coderr.ClusterAlreadyExists, "cluster already exists")
	ErrClusterNotFound      = coderr.NewCodeError(coderr.NotFound, "cluster not found")
	ErrClusterStateInvalid  = coderr.NewCodeError(coderr.Internal, "cluster state invalid")
	ErrSchemaNotFound       = coderr.NewCodeError(coderr.NotFound, "schema not found")
	ErrTableNotFound        = coderr.NewCodeError(coderr.NotFound, "table not found")
	ErrShardNotFound        = coderr.NewCodeError(coderr.NotFound, "shard not found")
	ErrNodeNotFound         = coderr.NewCodeError(coderr.NotFound, "NodeName not found")
	ErrTableAlreadyExists   = coderr.NewCodeError(coderr.Internal, "table already exists")
	ErrOpenTable            = coderr.NewCodeError(coderr.Internal, "open table")
	ErrParseTopologyType    = coderr.NewCodeError(coderr.Internal, "parse topology type")
)
