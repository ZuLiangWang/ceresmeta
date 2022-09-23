// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"

	"github.com/looplab/fsm"
)

type State string

const (
	StateInit      = "init"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type Type uint

const (
	Create Type = iota
	Delete
	TransferLeader
	Migrate
	Split
	Merge
	Scatter
)

// Procedure is used to describe how to execute a set of operations from the scheduler, e.g. SwitchLeaderProcedure, MergeShardProcedure.
type Procedure interface {
	// ID of the procedure.
	ID() uint64

	// Type of the procedure.
	Type() Type

	// Start the procedure.
	Start(ctx context.Context) error

	// Cancel the procedure.
	Cancel(ctx context.Context) error

	// State of the procedure. Retrieve the state of this procedure.
	State() State
}

// nolint
type Manager struct {
	storage    *Storage
	procedures []Procedure
}

func NewProcedure(operationType Type) *fsm.FSM {
	switch operationType {
	case Create:
		return nil
	case Delete:
		return nil
	case TransferLeader:
		return nil
	case Migrate:
		return nil
	case Split:
		return nil
	case Merge:
		return nil
	case Scatter:
		return nil
	}

	return nil
}
