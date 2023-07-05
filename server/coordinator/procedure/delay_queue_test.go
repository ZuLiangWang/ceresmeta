// Copyright 2023 CeresDB Project Authors. Licensed under Apache-2.0.

package procedure

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestProcedure struct{ ProcedureID uint64 }

func (t TestProcedure) RelatedVersionInfo() RelatedVersionInfo {
	return RelatedVersionInfo{}
}

func (t TestProcedure) Priority() Priority {
	return PriorityLow
}

func (t TestProcedure) ID() uint64 {
	return t.ProcedureID
}

func (t TestProcedure) Typ() Typ {
	return CreateTable
}

func (t TestProcedure) Start(_ context.Context) error {
	return nil
}

func (t TestProcedure) Cancel(_ context.Context) error {
	return nil
}

func (t TestProcedure) State() State {
	return StateInit
}

func TestDelayQueue(t *testing.T) {
	re := require.New(t)

	testProcedure0 := TestProcedure{ProcedureID: 0}
	testProcedure1 := TestProcedure{ProcedureID: 1}
	testProcedure2 := TestProcedure{ProcedureID: 2}
	testProcedure3 := TestProcedure{ProcedureID: 3}

	queue := NewProcedureDelayQueue(3)
	err := queue.Push(testProcedure0, time.Millisecond*40)
	re.NoError(err)
	err = queue.Push(testProcedure0, time.Millisecond*30)
	re.Error(err)
	err = queue.Push(testProcedure1, time.Millisecond*10)
	re.NoError(err)
	err = queue.Push(testProcedure2, time.Millisecond*20)
	re.NoError(err)
	err = queue.Push(testProcedure3, time.Millisecond*20)
	re.Error(err)
	re.Equal(3, queue.Len())

	po := queue.Pop()
	re.Nil(po)

	time.Sleep(time.Millisecond * 100)

	p0 := queue.Pop()
	re.Equal(uint64(1), p0.ID())
	p1 := queue.Pop()
	re.Equal(uint64(2), p1.ID())
	p2 := queue.Pop()
	re.Equal(uint64(0), p2.ID())
	p := queue.Pop()
	re.Nil(p)

	err = queue.Push(testProcedure0, time.Millisecond*20)
	re.NoError(err)

	time.Sleep(time.Millisecond * 10)
	p0 = queue.Pop()
	re.Nil(p0)

	time.Sleep(time.Millisecond * 10)
	p0 = queue.Pop()
	re.Equal(uint64(0), p0.ID())
}
