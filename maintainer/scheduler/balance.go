// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// balanceScheduler is used to check the balance status of all spans among all nodes
type balanceScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager

	random               *rand.Rand
	lastRebalanceTime    time.Time
	checkBalanceInterval time.Duration
	// forceBalance forces the scheduler to produce schedule tasks regardless of
	// `checkBalanceInterval`.
	// It is set to true when the last time `Schedule` produces some tasks,
	// and it is likely there are more tasks will be produced in the next
	// `Schedule`.
	// It speeds up rebalance.
	forceBalance bool
}

func NewBalanceScheduler(
	changefeedID common.ChangeFeedID, batchSize int,
	oc *operator.Controller, sc *span.Controller,
	balanceInterval time.Duration,
) *balanceScheduler {
	return &balanceScheduler{
		changefeedID:         changefeedID,
		batchSize:            batchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())),
		operatorController:   oc,
		spanController:       sc,
		nodeManager:          appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		checkBalanceInterval: balanceInterval,
		lastRebalanceTime:    time.Now(),
	}
}

func (s *balanceScheduler) Execute() time.Time {
	if !s.forceBalance && time.Since(s.lastRebalanceTime) < s.checkBalanceInterval {
		return s.lastRebalanceTime.Add(s.checkBalanceInterval)
	}
	now := time.Now()

	failpoint.Inject("StopBalanceScheduler", func() {
		failpoint.Return(now.Add(s.checkBalanceInterval))
	})

	// TODO: consider to ignore split tables' dispatcher basic schedule operator to decide whether we can make balance schedule
	if s.operatorController.OperatorSize() > 0 || s.spanController.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return now.Add(s.checkBalanceInterval)
	}

	nodes := s.nodeManager.GetAliveNodes()
	moved := s.schedulerGroup(nodes)
	if moved == 0 {
		// all groups are balanced, safe to do the global balance
		moved = s.schedulerGlobal(nodes)
	}

	s.forceBalance = moved >= s.batchSize
	s.lastRebalanceTime = time.Now()

	return now.Add(s.checkBalanceInterval)
}

func (s *balanceScheduler) Name() string {
	return "balance-scheduler"
}

func (s *balanceScheduler) schedulerGroup(nodes map[node.ID]*node.Info) int {
	batch, moved := s.batchSize, 0
	for _, group := range s.spanController.GetGroups() {
		// fast path, check the balance status
		moveSize := pkgScheduler.CheckBalanceStatus(s.spanController.GetTaskSizePerNodeByGroup(group), nodes)
		if moveSize <= 0 {
			// no need to do the balance, skip
			continue
		}
		replicas := s.spanController.GetReplicatingByGroup(group)
		moved += pkgScheduler.Balance(batch, s.random, nodes, replicas, s.doMove)
		if moved >= batch {
			break
		}
	}
	return moved
}

// TODO: refactor and simplify the implementation and limit max group size
func (s *balanceScheduler) schedulerGlobal(nodes map[node.ID]*node.Info) int {
	// fast path, check the balance status
	moveSize := pkgScheduler.CheckBalanceStatus(s.spanController.GetTaskSizePerNode(), nodes)
	if moveSize <= 0 {
		// no need to do the balance, skip
		return 0
	}
	groupNodetasks, valid := s.spanController.GetImbalanceGroupNodeTask(nodes)
	if !valid {
		// no need to do the balance, skip
		return 0
	}

	// complexity note: len(nodes) * len(groups)
	totalTasks := 0
	sizePerNode := make(map[node.ID]int, len(nodes))
	for _, nodeTasks := range groupNodetasks {
		for id, task := range nodeTasks {
			if task != nil {
				totalTasks++
				sizePerNode[id]++
			}
		}
	}
	lowerLimitPerNode := int(math.Floor(float64(totalTasks) / float64(len(nodes))))
	limitCnt := 0
	for _, size := range sizePerNode {
		if size == lowerLimitPerNode {
			limitCnt++
		}
	}
	if limitCnt == len(nodes) {
		// all nodes are global balanced
		return 0
	}

	moved := 0
	for _, nodeTasks := range groupNodetasks {
		availableNodes, victims, next := []node.ID{}, []node.ID{}, 0
		for id, task := range nodeTasks {
			if task != nil && sizePerNode[id] > lowerLimitPerNode {
				victims = append(victims, id)
			} else if task == nil && sizePerNode[id] < lowerLimitPerNode {
				availableNodes = append(availableNodes, id)
			}
		}

		for _, new := range availableNodes {
			if next >= len(victims) {
				break
			}
			old := victims[next]
			if s.doMove(nodeTasks[old], new) {
				sizePerNode[old]--
				sizePerNode[new]++
				next++
				moved++
			}
		}
	}
	log.Info("finish global balance", zap.Stringer("changefeed", s.changefeedID), zap.Int("moved", moved))
	return moved
}

func (s *balanceScheduler) doMove(replication *replica.SpanReplication, id node.ID) bool {
	op := operator.NewMoveDispatcherOperator(s.spanController, replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}
