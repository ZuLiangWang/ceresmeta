// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package cluster

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/CeresDB/ceresmeta/server/id"
	"github.com/CeresDB/ceresmeta/server/storage"
	"github.com/pkg/errors"
)

// TopologyManager manages the cluster topology, including the mapping relationship between shards, nodes, and tables.
type TopologyManager interface {
	// Load load cluster topology from storage.
	Load(ctx context.Context) error
	// GetVersion get cluster view version.
	GetVersion() uint64
	// GetClusterState get cluster view state.
	GetClusterState() storage.ClusterState
	// GetTableIDs get shardNode and tablesIDs with shardID and nodeName.
	GetTableIDs(shardIDs []storage.ShardID, nodeName string) map[storage.ShardID]ShardTableIDs
	// AddTable add table to cluster topology.
	AddTable(ctx context.Context, nodeName string, table storage.Table) (ShardVersionUpdate, error)
	// RemoveTable remove table from cluster topology.
	RemoveTable(ctx context.Context, tableID storage.TableID) (ShardVersionUpdate, error)
	// UpdateShardTables update table mapping with ShardTables
	UpdateShardTables(ctx context.Context, shardTables ShardTables) (ShardVersionUpdate, error)
	// GetShardNodesByID get shardNodes with shardID.
	GetShardNodesByID(shardID storage.ShardID) ([]storage.ShardNode, error)
	// GetShardNodesByTableIDs get shardNodes with tableIDs.
	GetShardNodesByTableIDs(tableID []storage.TableID) (GetShardNodesByTableIDsResult, error)
	// GetShardNodes get all shardNodes in cluster topology.
	GetShardNodes() GetShardNodesResult
	// InitClusterView init cluster view when create new cluster.
	InitClusterView(ctx context.Context) error
	// UpdateClusterView update cluster view with shardNodes.
	UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error
	// CreateShardViews create shardViews.
	CreateShardViews(ctx context.Context, shardViews []CreateShardView) error
}

type ShardTableIDs struct {
	ShardNode storage.ShardNode
	TableIDs  []storage.TableID
	Version   uint64
}

type GetShardTablesByNodeResult struct {
	ShardTableIDs map[storage.ShardID]ShardTableIDs
}

type GetShardNodesByTableIDsResult struct {
	ShardNodes map[storage.TableID][]storage.ShardNode
	Version    map[storage.ShardID]uint64
}

type GetShardNodesResult struct {
	shardNodes []storage.ShardNode
	versions   map[storage.ShardID]uint64
}

type CreateShardView struct {
	ShardID storage.ShardID
	Tables  []storage.TableID
}

type TopologyManagerImpl struct {
	storage      storage.Storage
	clusterID    storage.ClusterID
	shardIDAlloc id.Allocator

	// RWMutex is used to protect following fields.
	lock              sync.RWMutex
	clusterView       storage.ClusterView                     // ClusterView in memory.
	shardNodesMapping map[storage.ShardID][]storage.ShardNode // ShardID -> nodes of the shard
	nodeShardsMapping map[string][]storage.ShardNode          // nodeName -> shards of the NodeName
	// ShardView in memory.
	shardTablesMapping map[storage.ShardID]*storage.ShardView // ShardID -> shardTopology
	tableShardMapping  map[storage.TableID]storage.ShardID    // tableID -> ShardID

	nodes map[string]storage.Node // NodeName in memory.
}

func NewTopologyManagerImpl(storage storage.Storage, clusterID storage.ClusterID, shardIDAlloc id.Allocator) TopologyManager {
	return &TopologyManagerImpl{
		storage:      storage,
		clusterID:    clusterID,
		shardIDAlloc: shardIDAlloc,
	}
}

func (m *TopologyManagerImpl) Load(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.loadClusterView(ctx); err != nil {
		return errors.WithMessage(err, "load cluster view")
	}

	if err := m.loadShardViews(ctx); err != nil {
		return errors.WithMessage(err, "load shard views")
	}

	if err := m.loadNodes(ctx); err != nil {
		return errors.WithMessage(err, "load nodes")
	}
	return nil
}

func (m *TopologyManagerImpl) GetVersion() uint64 {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.clusterView.Version
}

func (m *TopologyManagerImpl) GetClusterState() storage.ClusterState {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.clusterView.State
}

func (m *TopologyManagerImpl) GetTableIDs(shardIDs []storage.ShardID, nodeName string) map[storage.ShardID]ShardTableIDs {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardTableIDs := make(map[storage.ShardID]ShardTableIDs, len(shardIDs))
	for _, shardID := range shardIDs {
		for _, shardNode := range m.shardNodesMapping[shardID] {
			if shardNode.NodeName == nodeName {
				shardView := m.shardTablesMapping[shardID]

				shardTableIDs[shardID] = ShardTableIDs{
					ShardNode: shardNode,
					TableIDs:  shardView.TableIDs,
					Version:   shardView.Version,
				}
				break
			}
		}
	}

	return shardTableIDs
}

func (m *TopologyManagerImpl) AddTable(ctx context.Context, nodeName string, table storage.Table) (ShardVersionUpdate, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Pick up one shard to contain the table.
	shardNodes := m.nodeShardsMapping[nodeName]
	var shardIDs []storage.ShardID
	for _, shardNode := range shardNodes {
		if shardNode.NodeName == nodeName && shardNode.ShardRole == storage.ShardRoleLeader {
			shardIDs = append(shardIDs, shardNode.ID)
		}
	}
	idx := rand.Int31n(int32(len((shardIDs)))) // #nosec G404
	shardID := shardIDs[idx]

	shardView := m.shardTablesMapping[shardID]
	prevVersion := shardView.Version

	tableIDs := make([]storage.TableID, 0, len(shardView.TableIDs)+1)
	tableIDs = append(tableIDs, shardView.TableIDs...)
	tableIDs = append(tableIDs, table.ID)

	newShardView := storage.ShardView{
		ShardID:   shardID,
		Version:   prevVersion + 1,
		TableIDs:  tableIDs,
		CreatedAt: uint64(time.Now().UnixMilli()),
	}

	// Update shard view in storage.
	err := m.storage.UpdateShardView(ctx, storage.UpdateShardViewRequest{
		ClusterID:     m.clusterID,
		ShardView:     newShardView,
		LatestVersion: prevVersion,
	})
	if err != nil {
		return ShardVersionUpdate{}, errors.WithMessage(err, "storage update shard view")
	}

	// Update shard view in memory.
	m.shardTablesMapping[shardID] = &newShardView
	m.tableShardMapping[table.ID] = shardID

	return ShardVersionUpdate{
		ShardID:     shardID,
		CurrVersion: prevVersion + 1,
		PrevVersion: prevVersion,
	}, nil
}

func (m *TopologyManagerImpl) RemoveTable(ctx context.Context, tableID storage.TableID) (ShardVersionUpdate, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	shardID, ok := m.tableShardMapping[tableID]
	if !ok {
		return ShardVersionUpdate{}, ErrTableNotFound.WithCausef("table id:%d", tableID)
	}

	shardView, ok := m.shardTablesMapping[shardID]
	if !ok {
		return ShardVersionUpdate{}, ErrShardNotFound.WithCausef("shard id:%d", shardID)
	}
	prevVersion := shardView.Version

	tableIDs := make([]storage.TableID, 0, len(shardView.TableIDs))
	for _, id := range shardView.TableIDs {
		if id != tableID {
			tableIDs = append(tableIDs, id)
		}
	}

	// Update shardView in storage.
	if err := m.storage.UpdateShardView(ctx, storage.UpdateShardViewRequest{
		ClusterID: m.clusterID,
		ShardView: storage.ShardView{
			ShardID:   shardView.ShardID,
			Version:   prevVersion + 1,
			TableIDs:  tableIDs,
			CreatedAt: uint64(time.Now().UnixMilli()),
		},
		LatestVersion: prevVersion,
	}); err != nil {
		return ShardVersionUpdate{}, errors.WithMessage(err, "storage update shard view")
	}

	// Update shardView in memory.
	shardView.Version = prevVersion + 1
	shardView.TableIDs = tableIDs
	delete(m.tableShardMapping, tableID)

	return ShardVersionUpdate{
		ShardID:     shardID,
		CurrVersion: prevVersion + 1,
		PrevVersion: prevVersion,
	}, nil
}

func (m *TopologyManagerImpl) UpdateShardTables(ctx context.Context, shardTables ShardTables) (ShardVersionUpdate, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if _, ok := m.shardNodesMapping[shardTables.Shard.ID]; !ok {
		return ShardVersionUpdate{}, ErrShardNotFound.WithCausef("shard id:%d", shardTables.Shard.ID)
	}

	prevVersion := uint64(0)
	shardView, ok := m.shardTablesMapping[shardTables.Shard.ID]
	if ok {
		prevVersion = shardView.Version
	}

	tableIDs := make([]storage.TableID, 0, len(shardTables.Tables))
	for _, table := range shardTables.Tables {
		tableIDs = append(tableIDs, table.ID)
	}

	if err := m.storage.UpdateShardView(ctx, storage.UpdateShardViewRequest{
		ClusterID: m.clusterID,
		ShardView: storage.ShardView{
			ShardID:   shardTables.Shard.ID,
			Version:   prevVersion + 1,
			TableIDs:  tableIDs,
			CreatedAt: uint64(time.Now().UnixMilli()),
		},
		LatestVersion: prevVersion,
	}); err != nil {
		return ShardVersionUpdate{}, errors.WithMessage(err, "storage update shard view")
	}

	// Update shardView in memory.
	if err := m.loadShardViews(ctx); err != nil {
		return ShardVersionUpdate{}, errors.WithMessage(err, "load shard views")
	}

	return ShardVersionUpdate{
		ShardID:     shardTables.Shard.ID,
		CurrVersion: prevVersion + 1,
		PrevVersion: prevVersion,
	}, nil
}

func (m *TopologyManagerImpl) GetShardNodesByID(shardID storage.ShardID) ([]storage.ShardNode, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardNodes, ok := m.shardNodesMapping[shardID]
	if !ok {
		return nil, ErrShardNotFound.WithCausef("shard id:%d", shardID)
	}

	return shardNodes, nil
}

func (m *TopologyManagerImpl) GetShardNodesByTableIDs(tableIDs []storage.TableID) (GetShardNodesByTableIDsResult, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	tableShardNodes := make(map[storage.TableID][]storage.ShardNode, len(tableIDs))
	shardViewVersions := make(map[storage.ShardID]uint64, 0)
	for _, tableID := range tableIDs {
		shardID, ok := m.tableShardMapping[tableID]
		if !ok {
			return GetShardNodesByTableIDsResult{}, ErrTableNotFound.WithCausef("table id:%d", tableID)
		}

		shardNodes, ok := m.shardNodesMapping[shardID]
		if !ok {
			return GetShardNodesByTableIDsResult{}, ErrShardNotFound.WithCausef("shard id:%d", shardID)
		}
		tableShardNodes[tableID] = shardNodes

		_, ok = shardViewVersions[shardID]
		if !ok {
			shardViewVersions[shardID] = m.shardTablesMapping[shardID].Version
		}
	}

	return GetShardNodesByTableIDsResult{
		ShardNodes: tableShardNodes,
		Version:    shardViewVersions,
	}, nil
}

func (m *TopologyManagerImpl) GetShardNodes() GetShardNodesResult {
	m.lock.RLock()
	defer m.lock.RUnlock()

	shardNodes := make([]storage.ShardNode, 0, len(m.shardNodesMapping))
	shardViewVersions := make(map[storage.ShardID]uint64, len(m.shardTablesMapping))
	for _, shardNode := range m.shardNodesMapping {
		shardNodes = append(shardNodes, shardNode...)
	}
	for shardID, shardView := range m.shardTablesMapping {
		shardViewVersions[shardID] = shardView.Version
	}

	return GetShardNodesResult{
		shardNodes: shardNodes,
		versions:   shardViewVersions,
	}
}

func (m *TopologyManagerImpl) InitClusterView(ctx context.Context) error {
	clusterView := storage.ClusterView{
		ClusterID:  m.clusterID,
		Version:    0,
		State:      storage.ClusterStateEmpty,
		ShardNodes: nil,
		CreatedAt:  uint64(time.Now().UnixMilli()),
	}

	err := m.storage.CreateClusterView(ctx, storage.CreateClusterViewRequest{ClusterView: clusterView})
	if err != nil {
		return errors.WithMessage(err, "storage create cluster view")
	}
	return nil
}

func (m *TopologyManagerImpl) UpdateClusterView(ctx context.Context, state storage.ClusterState, shardNodes []storage.ShardNode) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Update cluster view in storage.
	newClusterView := storage.ClusterView{
		ClusterID:  m.clusterID,
		Version:    m.clusterView.Version + 1,
		State:      state,
		ShardNodes: shardNodes,
		CreatedAt:  uint64(time.Now().UnixMilli()),
	}
	if err := m.storage.UpdateClusterView(ctx, storage.UpdateClusterViewRequest{
		ClusterID:     m.clusterID,
		ClusterView:   newClusterView,
		LatestVersion: m.clusterView.Version,
	}); err != nil {
		return errors.WithMessage(err, "storage update cluster view")
	}

	// Load cluster view into memory.
	if err := m.loadClusterView(ctx); err != nil {
		return errors.WithMessage(err, "load cluster view")
	}
	return nil
}

func (m *TopologyManagerImpl) CreateShardViews(ctx context.Context, createShardViews []CreateShardView) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Create shard view in storage.
	shardViews := make([]storage.ShardView, 0, len(createShardViews))
	for _, createShardView := range createShardViews {
		shardViews = append(shardViews, storage.ShardView{
			ShardID:   createShardView.ShardID,
			Version:   0,
			TableIDs:  createShardView.Tables,
			CreatedAt: uint64(time.Now().UnixMilli()),
		})
	}
	if err := m.storage.CreateShardViews(ctx, storage.CreateShardViewsRequest{
		ClusterID:  m.clusterID,
		ShardViews: shardViews,
	}); err != nil {
		return errors.WithMessage(err, "storage create shard view")
	}

	// Load shard view into memory.
	if err := m.loadShardViews(ctx); err != nil {
		return errors.WithMessage(err, "load shard view")
	}
	return nil
}

func (m *TopologyManagerImpl) loadClusterView(ctx context.Context) error {
	clusterViewResult, err := m.storage.GetClusterView(ctx, storage.GetClusterViewRequest{
		ClusterID: m.clusterID,
	})
	if err != nil {
		return errors.WithMessage(err, "storage get cluster view")
	}

	m.shardNodesMapping = make(map[storage.ShardID][]storage.ShardNode, len(clusterViewResult.ClusterView.ShardNodes))
	m.nodeShardsMapping = make(map[string][]storage.ShardNode, len(clusterViewResult.ClusterView.ShardNodes))
	for _, shardNode := range clusterViewResult.ClusterView.ShardNodes {
		m.shardNodesMapping[shardNode.ID] = append(m.shardNodesMapping[shardNode.ID], shardNode)
		m.nodeShardsMapping[shardNode.NodeName] = append(m.nodeShardsMapping[shardNode.NodeName], shardNode)
	}
	m.clusterView = clusterViewResult.ClusterView

	return nil
}

func (m *TopologyManagerImpl) loadShardViews(ctx context.Context) error {
	shardIDs := make([]storage.ShardID, 0, len(m.shardNodesMapping))
	for id := range m.shardNodesMapping {
		shardIDs = append(shardIDs, id)
	}

	shardViewsResult, err := m.storage.ListShardViews(ctx, storage.ListShardViewsRequest{
		ClusterID: m.clusterID,
		ShardIDs:  shardIDs,
	})
	if err != nil {
		return errors.WithMessage(err, "storage list shard views")
	}

	// Reset data in memory.
	m.shardTablesMapping = make(map[storage.ShardID]*storage.ShardView, len(shardViewsResult.ShardViews))
	m.tableShardMapping = make(map[storage.TableID]storage.ShardID, 0)
	for _, shardView := range shardViewsResult.ShardViews {
		view := shardView
		m.shardTablesMapping[shardView.ShardID] = &view
		for _, tableID := range shardView.TableIDs {
			m.tableShardMapping[tableID] = shardView.ShardID
		}
	}

	return nil
}

func (m *TopologyManagerImpl) loadNodes(ctx context.Context) error {
	nodesResult, err := m.storage.ListNodes(ctx, storage.ListNodesRequest{ClusterID: m.clusterID})
	if err != nil {
		return errors.WithMessage(err, "storage list nodes")
	}

	// Reset data in memory.
	m.nodes = make(map[string]storage.Node, len(nodesResult.Nodes))
	for _, node := range nodesResult.Nodes {
		m.nodes[node.Name] = node
	}

	return nil
}
