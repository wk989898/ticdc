// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatchermanager

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

type DispatcherMap struct {
	m sync.Map
	// sequence number is increasing when dispatcher is added.
	//
	// Seq is used to prevent the fallback of changefeed's checkpointTs.
	// When some new dispatcher(table) is being added, the maintainer will block the forward of changefeed's checkpointTs
	// until the maintainer receive the message that the new dispatcher's component status change to working.
	//
	// Besides, there is no strict order of the heartbeat message and the table status messages, which is means
	// it can happen that when dispatcher A is created, event dispatcher manager may first send a table status message
	// to show the new dispatcher is working, and then send a heartbeat message of the current watermark,
	// which is calculated without the new disaptcher.
	// When the checkpointTs of the watermark is large than the startTs of the new dispatcher,
	// the watermark of next heartbeat, which calculated with the new dispatcher can be less than the previous watermark.
	// Then it can cause the fallback of changefeed's checkpointTs.
	// To avoid fallback, we add a seq number in each heartbeat message(both collect from collectComponentStatusWhenChanged and aggregateDispatcherHeartbeats)
	// When a table is added the seq number will be increase,
	// and when the maintainer receive the outdate seq, it will know the heartbeat message is outdate and ignore it.
	// In this way, even the above case happens, the changefeed's checkpointTs will not fallback.
	//
	// Here we don't need to make seq changes always atmoic with the m changed.
	// Our target is just :
	// The seq get from ForEach should be smaller than the seq get from Set
	// when ForEach is not access the new dispatcher just Set.
	// So we add seq after the dispatcher is add in the m for Set, and get the seq before do range for ForRange.
	seq atomic.Uint64
}

func newDispatcherMap() *DispatcherMap {
	dispatcherMap := &DispatcherMap{
		m: sync.Map{},
	}
	dispatcherMap.seq.Store(0)
	return dispatcherMap
}

func (d *DispatcherMap) Len() int {
	len := 0
	d.m.Range(func(_, _ interface{}) bool {
		len++
		return true
	})
	return len
}

func (d *DispatcherMap) Get(id common.DispatcherID) (*dispatcher.Dispatcher, bool) {
	dispatcherItem, ok := d.m.Load(id)
	if ok {
		return dispatcherItem.(*dispatcher.Dispatcher), ok
	}
	return nil, false
}

func (d *DispatcherMap) GetSeq() uint64 {
	return d.seq.Load()
}

func (d *DispatcherMap) Delete(id common.DispatcherID) {
	d.m.Delete(id)
}

func (d *DispatcherMap) Set(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) uint64 {
	d.m.Store(id, dispatcher)
	d.seq.Add(1)
	return d.seq.Load()
}

func (d *DispatcherMap) ForEach(fn func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher)) uint64 {
	seq := d.seq.Load()
	d.m.Range(func(key, value interface{}) bool {
		fn(key.(common.DispatcherID), value.(*dispatcher.Dispatcher))
		return true
	})
	return seq
}

func toFilterConfigPB(filter *config.FilterConfig) *eventpb.InnerFilterConfig {
	filterConfig := &eventpb.InnerFilterConfig{
		Rules:            filter.Rules,
		IgnoreTxnStartTs: filter.IgnoreTxnStartTs,
		EventFilters:     make([]*eventpb.EventFilterRule, 0),
	}

	for _, eventFilterRule := range filter.EventFilters {
		filterConfig.EventFilters = append(filterConfig.EventFilters, toEventFilterRulePB(eventFilterRule))
	}

	return filterConfig
}

func toEventFilterRulePB(rule *config.EventFilterRule) *eventpb.EventFilterRule {
	eventFilterPB := &eventpb.EventFilterRule{
		IgnoreInsertValueExpr:    rule.IgnoreInsertValueExpr,
		IgnoreUpdateNewValueExpr: rule.IgnoreUpdateNewValueExpr,
		IgnoreUpdateOldValueExpr: rule.IgnoreUpdateOldValueExpr,
		IgnoreDeleteValueExpr:    rule.IgnoreDeleteValueExpr,
	}

	eventFilterPB.Matcher = append(eventFilterPB.Matcher, rule.Matcher...)

	for _, ignoreEvent := range rule.IgnoreEvent {
		eventFilterPB.IgnoreEvent = append(eventFilterPB.IgnoreEvent, string(ignoreEvent))
	}

	eventFilterPB.IgnoreSql = append(eventFilterPB.IgnoreSql, rule.IgnoreSQL...)

	return eventFilterPB
}

type Watermark struct {
	mutex sync.Mutex
	*heartbeatpb.Watermark
}

func NewWatermark(ts uint64) Watermark {
	return Watermark{
		Watermark: &heartbeatpb.Watermark{
			CheckpointTs: ts,
			ResolvedTs:   ts,
		},
	}
}

func (w *Watermark) Get() *heartbeatpb.Watermark {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.Watermark
}

func (w *Watermark) Set(watermark *heartbeatpb.Watermark) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.Watermark = watermark
}

func newSchedulerDispatcherRequestDynamicStream() dynstream.DynamicStream[int, common.GID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler] {
	ds := dynstream.NewParallelDynamicStream(
		func(id common.GID) uint64 { return id.FastHash() },
		&SchedulerDispatcherRequestHandler{}, dynstream.NewOption())
	ds.Start()
	return ds
}

type SchedulerDispatcherRequest struct {
	*heartbeatpb.ScheduleDispatcherRequest
}

func NewSchedulerDispatcherRequest(req *heartbeatpb.ScheduleDispatcherRequest) SchedulerDispatcherRequest {
	return SchedulerDispatcherRequest{req}
}

type SchedulerDispatcherRequestHandler struct{}

func (h *SchedulerDispatcherRequestHandler) Path(scheduleDispatcherRequest SchedulerDispatcherRequest) common.GID {
	return common.NewChangefeedGIDFromPB(scheduleDispatcherRequest.ChangefeedID)
}

func (h *SchedulerDispatcherRequestHandler) Handle(eventDispatcherManager *EventDispatcherManager, reqs ...SchedulerDispatcherRequest) bool {
	// If req is about remove dispatcher, then there will only be one request in reqs.
	infos := make([]dispatcherCreateInfo, 0, len(reqs))
	for _, req := range reqs {
		if req.ScheduleDispatcherRequest == nil {
			log.Warn("scheduleDispatcherRequest is nil, skip")
			continue
		}
		config := req.Config
		dispatcherID := common.NewDispatcherIDFromPB(config.DispatcherID)
		switch req.ScheduleAction {
		case heartbeatpb.ScheduleAction_Create:
			infos = append(infos, dispatcherCreateInfo{
				Id:        dispatcherID,
				TableSpan: config.Span,
				StartTs:   config.StartTs,
				SchemaID:  config.SchemaID,
			})
		case heartbeatpb.ScheduleAction_Remove:
			if len(reqs) != 1 {
				log.Error("invalid remove dispatcher request count in one batch", zap.Int("count", len(reqs)))
			}
			eventDispatcherManager.removeDispatcher(dispatcherID)
		}
	}
	if len(infos) > 0 {
		err := eventDispatcherManager.newDispatchers(infos, false)
		if err != nil {
			select {
			case eventDispatcherManager.errCh <- err:
				log.Error("new dispatcher meet error", zap.String("ChangefeedID", eventDispatcherManager.changefeedID.String()),
					zap.Error(err))
			default:
				log.Error("error channel is full, discard error",
					zap.String("ChangefeedID", eventDispatcherManager.changefeedID.String()),
					zap.Error(err))
			}
		}
	}
	return false
}

func (h *SchedulerDispatcherRequestHandler) GetSize(event SchedulerDispatcherRequest) int { return 0 }
func (h *SchedulerDispatcherRequestHandler) IsPaused(event SchedulerDispatcherRequest) bool {
	return false
}

func (h *SchedulerDispatcherRequestHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}

func (h *SchedulerDispatcherRequestHandler) GetTimestamp(event SchedulerDispatcherRequest) dynstream.Timestamp {
	return 0
}

func (h *SchedulerDispatcherRequestHandler) GetType(event SchedulerDispatcherRequest) dynstream.EventType {
	// we do batch for create dispatcher now.
	switch event.ScheduleAction {
	case heartbeatpb.ScheduleAction_Create:
		return dynstream.EventType{DataGroup: 1, Property: dynstream.BatchableData}
	case heartbeatpb.ScheduleAction_Remove:
		return dynstream.EventType{DataGroup: 2, Property: dynstream.NonBatchable}
	default:
		log.Panic("unknown schedule action", zap.Int("action", int(event.ScheduleAction)))
	}
	return dynstream.DefaultEventType
}

func (h *SchedulerDispatcherRequestHandler) OnDrop(event SchedulerDispatcherRequest) interface{} {
	return nil
}

func newHeartBeatResponseDynamicStream(dds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]) dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler] {
	ds := dynstream.NewParallelDynamicStream(
		func(id common.GID) uint64 { return id.FastHash() },
		newHeartBeatResponseHandler(dds))
	ds.Start()
	return ds
}

type HeartBeatResponse struct {
	*heartbeatpb.HeartBeatResponse
}

func NewHeartBeatResponse(resp *heartbeatpb.HeartBeatResponse) HeartBeatResponse {
	return HeartBeatResponse{resp}
}

type HeartBeatResponseHandler struct {
	dispatcherStatusDynamicStream dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]
}

func newHeartBeatResponseHandler(dds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]) *HeartBeatResponseHandler {
	return &HeartBeatResponseHandler{dispatcherStatusDynamicStream: dds}
}

func (h *HeartBeatResponseHandler) Path(HeartbeatResponse HeartBeatResponse) common.GID {
	return common.NewChangefeedGIDFromPB(HeartbeatResponse.ChangefeedID)
}

func (h *HeartBeatResponseHandler) Handle(eventDispatcherManager *EventDispatcherManager, resps ...HeartBeatResponse) bool {
	if len(resps) != 1 {
		// TODO: Support batch
		panic("invalid response count")
	}
	heartbeatResponse := resps[0]
	dispatcherStatuses := heartbeatResponse.GetDispatcherStatuses()
	for _, dispatcherStatus := range dispatcherStatuses {
		influencedDispatchersType := dispatcherStatus.InfluencedDispatchers.InfluenceType
		switch influencedDispatchersType {
		case heartbeatpb.InfluenceType_Normal:
			for _, dispatcherID := range dispatcherStatus.InfluencedDispatchers.DispatcherIDs {
				dispId := common.NewDispatcherIDFromPB(dispatcherID)
				h.dispatcherStatusDynamicStream.Push(
					dispId,
					dispatcher.NewDispatcherStatusWithID(dispatcherStatus, dispId))
			}
		case heartbeatpb.InfluenceType_DB:
			schemaID := dispatcherStatus.InfluencedDispatchers.SchemaID
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			dispatcherIds := eventDispatcherManager.GetAllDispatchers(schemaID)
			for _, id := range dispatcherIds {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				}
			}
		case heartbeatpb.InfluenceType_All:
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			eventDispatcherManager.GetDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.Dispatcher) {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				}
			})
		}
	}
	return false
}

func (h *HeartBeatResponseHandler) GetSize(event HeartBeatResponse) int   { return 0 }
func (h *HeartBeatResponseHandler) IsPaused(event HeartBeatResponse) bool { return false }
func (h *HeartBeatResponseHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}

func (h *HeartBeatResponseHandler) GetTimestamp(event HeartBeatResponse) dynstream.Timestamp {
	return 0
}

func (h *HeartBeatResponseHandler) GetType(event HeartBeatResponse) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *HeartBeatResponseHandler) OnDrop(event HeartBeatResponse) interface{} {
	return nil
}

// checkpointTsMessageDynamicStream is responsible for push checkpointTsMessage to the corresponding table trigger event dispatcher.
func newCheckpointTsMessageDynamicStream() dynstream.DynamicStream[int, common.GID, CheckpointTsMessage, *EventDispatcherManager, *CheckpointTsMessageHandler] {
	ds := dynstream.NewParallelDynamicStream(
		func(id common.GID) uint64 { return id.FastHash() },
		&CheckpointTsMessageHandler{})
	ds.Start()
	return ds
}

type CheckpointTsMessage struct {
	*heartbeatpb.CheckpointTsMessage
}

func NewCheckpointTsMessage(msg *heartbeatpb.CheckpointTsMessage) CheckpointTsMessage {
	return CheckpointTsMessage{msg}
}

type CheckpointTsMessageHandler struct{}

func (h *CheckpointTsMessageHandler) Path(checkpointTsMessage CheckpointTsMessage) common.GID {
	return common.NewChangefeedGIDFromPB(checkpointTsMessage.ChangefeedID)
}

func (h *CheckpointTsMessageHandler) Handle(eventDispatcherManager *EventDispatcherManager, messages ...CheckpointTsMessage) bool {
	if len(messages) != 1 {
		// TODO: Support batch
		panic("invalid message count")
	}
	checkpointTsMessage := messages[0]
	if eventDispatcherManager.tableTriggerEventDispatcher != nil {
		eventDispatcherManager.sink.AddCheckpointTs(checkpointTsMessage.CheckpointTs)
	}
	return false
}

func (h *CheckpointTsMessageHandler) GetSize(event CheckpointTsMessage) int   { return 0 }
func (h *CheckpointTsMessageHandler) IsPaused(event CheckpointTsMessage) bool { return false }
func (h *CheckpointTsMessageHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}

func (h *CheckpointTsMessageHandler) GetTimestamp(event CheckpointTsMessage) dynstream.Timestamp {
	return 0
}

func (h *CheckpointTsMessageHandler) GetType(event CheckpointTsMessage) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *CheckpointTsMessageHandler) OnDrop(event CheckpointTsMessage) interface{} {
	return nil
}

func newMergeDispatcherRequestDynamicStream() dynstream.DynamicStream[int, common.GID, MergeDispatcherRequest, *EventDispatcherManager, *MergeDispatcherRequestHandler] {
	ds := dynstream.NewParallelDynamicStream(
		func(id common.GID) uint64 { return id.FastHash() },
		&MergeDispatcherRequestHandler{})
	ds.Start()
	return ds
}

type MergeDispatcherRequest struct {
	*heartbeatpb.MergeDispatcherRequest
}

func NewMergeDispatcherRequest(req *heartbeatpb.MergeDispatcherRequest) MergeDispatcherRequest {
	return MergeDispatcherRequest{req}
}

type MergeDispatcherRequestHandler struct{}

func (h *MergeDispatcherRequestHandler) Path(mergeDispatcherRequest MergeDispatcherRequest) common.GID {
	return common.NewChangefeedGIDFromPB(mergeDispatcherRequest.ChangefeedID)
}

func (h *MergeDispatcherRequestHandler) Handle(eventDispatcherManager *EventDispatcherManager, reqs ...MergeDispatcherRequest) bool {
	if len(reqs) != 1 {
		panic("invalid request count")
	}

	mergeDispatcherRequest := reqs[0]
	dispatcherIDs := make([]common.DispatcherID, 0, len(mergeDispatcherRequest.DispatcherIDs))
	for _, id := range mergeDispatcherRequest.DispatcherIDs {
		dispatcherIDs = append(dispatcherIDs, common.NewDispatcherIDFromPB(id))
	}
	eventDispatcherManager.MergeDispatcher(dispatcherIDs, common.NewDispatcherIDFromPB(mergeDispatcherRequest.MergedDispatcherID))
	return false
}

func (h *MergeDispatcherRequestHandler) GetSize(event MergeDispatcherRequest) int   { return 0 }
func (h *MergeDispatcherRequestHandler) IsPaused(event MergeDispatcherRequest) bool { return false }
func (h *MergeDispatcherRequestHandler) GetArea(path common.GID, dest *EventDispatcherManager) int {
	return 0
}

func (h *MergeDispatcherRequestHandler) GetTimestamp(event MergeDispatcherRequest) dynstream.Timestamp {
	return 0
}

func (h *MergeDispatcherRequestHandler) GetType(event MergeDispatcherRequest) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *MergeDispatcherRequestHandler) OnDrop(event MergeDispatcherRequest) interface{} {
	return nil
}
