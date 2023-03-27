// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package queue

import (
	"context"
	"testing"
	"time"

	"github.com/CeresDB/ceresmeta/server/coordinator/procedure"
	"github.com/stretchr/testify/require"
)

type TestProcedure struct{ id uint64 }

func (t TestProcedure) ID() uint64 {
	return t.id
}

func (t TestProcedure) Typ() procedure.Typ {
	return procedure.CreateTable
}

func (t TestProcedure) Start(_ context.Context) error {
	return nil
}

func (t TestProcedure) Cancel(_ context.Context) error {
	return nil
}

func (t TestProcedure) State() procedure.State {
	return procedure.StateInit
}

func TestDelayQueue(t *testing.T) {
	re := require.New(t)

	testProcedure0 := TestProcedure{0}
	testProcedure1 := TestProcedure{1}
	testProcedure2 := TestProcedure{2}
	testProcedure3 := TestProcedure{3}

	queue := NewProcedureDelayQueue(3)
	err := queue.Push(testProcedure0, time.Second)
	re.NoError(err)
	err = queue.Push(testProcedure0, time.Second)
	re.Error(err)
	err = queue.Push(testProcedure1, time.Second)
	re.NoError(err)
	err = queue.Push(testProcedure2, time.Second)
	re.NoError(err)
	err = queue.Push(testProcedure3, time.Second)
	re.Error(err)
	re.Equal(3, queue.Len())

	po := queue.Pop()
	re.Nil(po)

	time.Sleep(time.Second)

	p0 := queue.Pop()
	re.Equal(uint64(0), p0.ID())
	p1 := queue.Pop()
	re.Equal(uint64(1), p1.ID())
	p2 := queue.Pop()
	re.Equal(uint64(2), p2.ID())
	p := queue.Pop()
	re.Nil(p)

	err = queue.Push(testProcedure0, time.Second*2)
	re.NoError(err)

	time.Sleep(time.Second)
	p0 = queue.Pop()
	re.Nil(p0)

	time.Sleep(time.Second)
	p0 = queue.Pop()
	re.Equal(uint64(0), p0.ID())
}
