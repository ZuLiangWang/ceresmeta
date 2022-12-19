// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
)

type State string

const (
	StateInit      = "init"
	StateRunning   = "running"
	StateFinished  = "finished"
	StateFailed    = "failed"
	StateCancelled = "cancelled"
)

type Typ uint

const (
	Create Typ = iota
	Delete
	TransferLeader
	Migrate
	Split
	Merge
	Scatter
	CreateTable
	DropTable
	CreatePartitionTable
)

// Procedure is used to describe how to execute a set of operations from the scheduler, e.g. SwitchLeaderProcedure, MergeShardProcedure.
type Procedure interface {
	// ID of the procedure.
	ID() uint64

	// Typ of the procedure.
	Typ() Typ

	// Start the procedure.
	Start(ctx context.Context) error

	// Cancel the procedure.
	Cancel(ctx context.Context) error

	// State of the procedure. Retrieve the state of this procedure.
	State() State
}

// Info is used to provide immutable description procedure information.
type Info struct {
	ID    uint64
	Typ   Typ
	State State
}
