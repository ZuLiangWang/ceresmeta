// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"sync"

	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/coordinator/eventdispatch"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	queueSize         = 10
	metaListBatchSize = 100
)

type ManagerImpl struct {
	// This lock is used to protect the field `procedures` and `running`.
	lock       sync.RWMutex
	procedures []Procedure
	running    bool

	dispatch       eventdispatch.Dispatch
	procedureQueue chan Procedure
}

func (m *ManagerImpl) Start(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.running {
		log.Warn("cluster manager has already been started")
		return nil
	}
	m.procedureQueue = make(chan Procedure, queueSize)
	go m.startProcedureWorker(ctx, m.procedureQueue)
	return nil
}

func (m *ManagerImpl) Stop(ctx context.Context) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	close(m.procedureQueue)
	for _, procedure := range m.procedures {
		if procedure.State() == StateRunning {
			err := procedure.Cancel(ctx)
			log.Error("cancel procedure failed", zap.Error(err), zap.Uint64("procedureID", procedure.ID()))
			// TODO: consider whether a single procedure cancel failed should return directly.
			return err
		}
	}
	return nil
}

func (m *ManagerImpl) Submit(_ context.Context, procedure Procedure) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.procedures) >= 1 {
		return errors.WithMessagef(ErrProcedureRunning, "submit procedure failed for exists running procedure, running procedureID:%d, submit procedureID:%d", m.procedures[0].ID(), procedure.ID())
	}
	m.procedures = append(m.procedures, procedure)
	m.procedureQueue <- procedure
	return nil
}

func (m *ManagerImpl) Cancel(ctx context.Context, procedureID uint64) error {
	procedure := m.removeProcedure(procedureID)
	if procedure == nil {
		log.Error("procedure not found", zap.Uint64("procedureID", procedureID))
		return ErrProcedureNotFound
	}
	err := procedure.Cancel(ctx)
	if err != nil {
		return errors.WithMessagef(ErrProcedureNotFound, "cancel procedure failed, procedureID:%d", procedureID)
	}
	return nil
}

func (m *ManagerImpl) ListRunningProcedure(_ context.Context) ([]*Info, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	procedureInfos := make([]*Info, 0)
	for _, procedure := range m.procedures {
		if procedure.State() == StateRunning {
			procedureInfos = append(procedureInfos, &Info{
				ID:    procedure.ID(),
				Typ:   procedure.Typ(),
				State: procedure.State(),
			})
		}
	}
	return procedureInfos, nil
}

func NewManagerImpl() (Manager, error) {
	manager := &ManagerImpl{
		dispatch: eventdispatch.NewDispatchImpl(),
	}
	return manager, nil
}

func (m *ManagerImpl) startProcedureWorker(ctx context.Context, procedures <-chan Procedure) {
	for procedure := range procedures {
		err := procedure.Start(ctx)
		if err != nil {
			log.Error("procedure start failed", zap.Error(err))
		}

		m.removeProcedure(procedure.ID())
	}
}

func (m *ManagerImpl) removeProcedure(id uint64) Procedure {
	m.lock.Lock()
	defer m.lock.Unlock()

	index := -1
	for i, p := range m.procedures {
		if p.ID() == id {
			index = i
			break
		}
	}
	if index != -1 {
		result := m.procedures[index]
		m.procedures = append(m.procedures[:index], m.procedures[index+1:]...)
		return result
	}
	return nil
}
