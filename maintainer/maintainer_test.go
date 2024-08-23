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

package maintainer

import (
	"context"
	"flag"
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/config"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	config2 "github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"net/http"
	"net/http/pprof"
	"strconv"
	"testing"
	"time"
)

type mockDispatcherManager struct {
	mc           messaging.MessageCenter
	dispatchers  []*heartbeatpb.TableSpanStatus
	msgCh        chan *messaging.TargetMessage
	maintainerID messaging.ServerId
	checkpointTs uint64
}

func MockDispatcherManager() *mockDispatcherManager {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	m := &mockDispatcherManager{
		mc:          mc,
		dispatchers: make([]*heartbeatpb.TableSpanStatus, 0, 1000000),
		msgCh:       make(chan *messaging.TargetMessage, 1024),
	}
	mc.RegisterHandler(messaging.DispatcherManagerManagerTopic, m.recvMessages)
	mc.RegisterHandler(messaging.HeartbeatCollectorTopic, m.recvMessages)
	return m
}

func (m *mockDispatcherManager) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.msgCh:
			m.handleMessage(msg)
		case <-tick.C:
			m.sendHeartbeat()
		}
	}
}

func (m *mockDispatcherManager) handleMessage(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeMaintainerBootstrapRequest:
		m.onBootstrapRequest(msg)
	case messaging.TypeScheduleDispatcherRequest:
		m.onDispatchRequest(msg)
	default:
		log.Panic("unknown msg type", zap.Any("msg", msg))
	}
}
func (m *mockDispatcherManager) sendMessages(msg *heartbeatpb.HeartBeatRequest) {
	target := messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		msg,
	)
	err := m.mc.SendCommand(target)
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
}
func (m *mockDispatcherManager) recvMessages(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	// receive message from coordinator
	case messaging.TypeScheduleDispatcherRequest,
		messaging.TypeMaintainerBootstrapRequest:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m.msgCh <- msg:
		}
		return nil
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}
func (m *mockDispatcherManager) onBootstrapRequest(msg *messaging.TargetMessage) {
	req := msg.Message[0].(*heartbeatpb.MaintainerBootstrapRequest)
	m.maintainerID = msg.From
	response := &heartbeatpb.MaintainerBootstrapResponse{
		ChangefeedID: req.ChangefeedID,
	}
	err := m.mc.SendCommand(messaging.NewSingleTargetMessage(
		m.maintainerID,
		messaging.MaintainerManagerTopic,
		response,
	))
	if err != nil {
		log.Warn("send command failed", zap.Error(err))
	}
	log.Info("New maintainer online",
		zap.String("server", m.maintainerID.String()))
}
func (m *mockDispatcherManager) onDispatchRequest(
	msg *messaging.TargetMessage,
) {
	request := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	if m.maintainerID != msg.From {
		log.Warn("ignore invalid maintainer id",
			zap.Any("request", request),
			zap.Any("maintainer", msg.From))
		return
	}
	if request.ScheduleAction == heartbeatpb.ScheduleAction_Create {
		m.dispatchers = append(m.dispatchers, &heartbeatpb.TableSpanStatus{
			Span:            request.Config.Span,
			ComponentStatus: heartbeatpb.ComponentState_Working,
			State:           nil,
			CheckpointTs:    0,
		})
	}
}

func (m *mockDispatcherManager) sendHeartbeat() {
	if m.maintainerID.String() != "" {
		response := &heartbeatpb.HeartBeatRequest{
			ChangefeedID: m.maintainerID.String(),
			Watermark: &heartbeatpb.Watermark{
				CheckpointTs: m.checkpointTs,
				ResolvedTs:   m.checkpointTs,
			},
			Statuses: m.dispatchers,
		}
		m.checkpointTs++
		m.sendMessages(response)
	}
}

func TestMaintainerSchedule(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	go func() {
		t.Fatal(http.ListenAndServe(":8300", mux))
	}()

	node := &common.NodeInfo{ID: uuid.New().String()}
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMessageCenter(ctx,
		messaging.ServerId(node.ID), 100, config.NewDefaultMessageCenterConfig()))
	appcontext.SetService(watcher.NodeManagerName, watcher.NewNodeManager(nil, nil))
	stream := dynstream.NewDynamicStreamDefault[string, *Event, *Maintainer](&StreamHandler{})
	stream.Start()
	cfID := model.DefaultChangeFeedID("test")
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	mc.RegisterHandler(messaging.MaintainerManagerTopic,
		func(ctx context.Context, msg *messaging.TargetMessage) error {
			stream.In() <- &Event{
				changefeedID: cfID.ID,
				eventType:    EventMessage,
				message:      msg,
			}
			return nil
		})
	dispatcherManager := MockDispatcherManager()
	go dispatcherManager.Run(ctx)

	taskScheduler := threadpool.NewTaskSchedulerDefault("maintainer")
	maintainer := NewMaintainer(cfID, &model.ChangeFeedInfo{
		Config: config2.GetDefaultReplicaConfig(),
	}, node, stream, taskScheduler, 10)
	_ = stream.AddPaths(dynstream.PathAndDest[string, *Maintainer]{
		Path: cfID.ID,
		Dest: maintainer,
	})

	if !flag.Parsed() {
		flag.Parse()
	}

	argList := flag.Args()
	if len(argList) > 1 {
		t.Fatal("unexpected args", argList)
	}
	tableSize := 100
	sleepTime := 5
	if len(argList) == 1 {
		tableSize, _ = strconv.Atoi(argList[0])
	}
	if len(argList) == 2 {
		tableSize, _ = strconv.Atoi(argList[0])
		sleepTime, _ = strconv.Atoi(argList[1])
	}

	for id := 0; id < tableSize; id++ {
		span := spanz.TableIDToComparableSpan(int64(id))
		tableSpan := &common.TableSpan{TableSpan: &heartbeatpb.TableSpan{
			TableID:  uint64(id),
			StartKey: span.StartKey,
			EndKey:   span.EndKey,
		}}
		replicaSet := NewReplicaSet(maintainer.id, tableSpan, maintainer.watermark.CheckpointTs).(*ReplicaSet)
		stm, _ := scheduler.NewStateMachine(tableSpan, nil, replicaSet)
		maintainer.scheduler.AddNewTask(stm)
	}
	// send bootstrap message
	maintainer.sendMessages(maintainer.bootstrapper.HandleNewNodes(
		map[common.NodeID]*common.NodeInfo{
			node.ID: node,
		},
	))
	// setup period event
	SubmitScheduledEvent(maintainer.taskScheduler, maintainer.stream, &Event{
		changefeedID: maintainer.id.ID,
		eventType:    EventPeriod,
	}, time.Now().Add(time.Millisecond*500))
	time.Sleep(time.Second * time.Duration(sleepTime))

	cancel()
	stream.Close()
	require.Equal(t, tableSize,
		maintainer.scheduler.GetTaskSizeByState(scheduler.SchedulerStatusWorking))
	require.Equal(t, tableSize,
		maintainer.scheduler.GetTaskSizeByNodeID(node.ID))
}