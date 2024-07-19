package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/flowbehappy/tigate/logservice/logpuller"
	"github.com/flowbehappy/tigate/logservice/upstream"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type EventObserver func(raw *common.RawKVEntry)

type WatermarkNotifier func(watermark uint64)

type EventStore interface {
	// add a callback to be called when a new event is added to the store;
	// but for old data this is not feasiable? may we can just return a current watermark when register
	RegisterDispatcher(
		dispatcherID common.DispatcherID,
		span tablepb.Span,
		startTS common.Ts,
		observer EventObserver,
		notifier WatermarkNotifier,
	) error

	UpdateDispatcherSendTS(dispatcherID common.DispatcherID, gcTS uint64) error

	UnregisterDispatcher(dispatcherID common.DispatcherID) error

	// TODO: ignore large txn now, so we can read all transactions of the same commit ts at one time
	// [startCommitTS, endCommitTS)?
	GetIterator(span tablepb.Span, startCommitTS uint64, endCommitTS uint64) (EventIterator, error)
}

type EventIterator interface {
	Next() (event []byte, isNewTxn bool, err error)

	// Close closes the iterator.
	Close() error
}

type eventWithTableID struct {
	span tablepb.Span
	raw  *common.RawKVEntry
}

type tableState struct {
	span     tablepb.Span
	observer EventObserver
	notifier WatermarkNotifier

	// data before this watermark won't be needed
	watermark atomic.Uint64

	ch chan eventWithTableID
}
type eventStore struct {
	dbs      []*pebble.DB
	channels []chan eventWithTableID
	puller   *logpuller.LogPuller

	gcManager *gcManager

	// To manage background goroutines.
	wg sync.WaitGroup

	mu sync.RWMutex
	// TODO: rename the following variables
	tables *spanz.HashMap[common.DispatcherID]
	spans  map[common.DispatcherID]*tableState
}

const dataDir = "event_store"
const dbCount = 32

func NewEventStore(ctx context.Context, up *upstream.Upstream, root string) EventStore {
	pdCli := up.PDClient
	regionCache := up.RegionCache
	pdClock := up.PDClock

	grpcPool := logpuller.NewConnAndClientPool(up.SecurityConfig)
	clientConfig := &logpuller.SharedClientConfig{
		KVClientWorkerConcurrent:     32,
		KVClientGrpcStreamConcurrent: 32,
		KVClientAdvanceIntervalInMs:  300,
	}
	client := logpuller.NewSharedClient(
		clientConfig,
		pdCli,
		grpcPool,
		regionCache,
		pdClock,
	)

	dbPath := fmt.Sprintf("%s/%s", root, dataDir)
	dbs := make([]*pebble.DB, 0, dbCount)
	channels := make([]chan eventWithTableID, 0, dbCount)
	// TODO: update pebble options
	// TODO: close pebble db at exit
	for i := 0; i < dbCount; i++ {
		db, err := pebble.Open(fmt.Sprintf("%s/%d", dbPath, i), &pebble.Options{})
		if err != nil {
			log.Fatal("open db failed", zap.Error(err))
		}
		dbs = append(dbs, db)
		channels = append(channels, make(chan eventWithTableID, 1024))
	}

	store := &eventStore{
		dbs:      dbs,
		channels: channels,

		gcManager: newGCManager(),

		tables: spanz.NewHashMap[common.DispatcherID](),
		spans:  make(map[common.DispatcherID]*tableState),
	}

	for i := range dbs {
		go func(index int) {
			store.handleEvents(ctx, dbs[index], channels[index])
		}(i)
	}

	consume := func(ctx context.Context, raw *common.RawKVEntry, span tablepb.Span) error {
		if raw != nil {
			store.writeEvent(span, raw)
		}
		return nil
	}
	pullerConfig := &logpuller.LogPullerConfig{
		WorkerCount:  len(dbs),
		HashSpanFunc: spanz.HashTableSpan,
	}
	puller := logpuller.NewLogPuller(client, consume, pullerConfig)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return puller.Run(ctx)
	})

	go store.gcManager.run(ctx, &store.wg, store.deleteEvents)

	store.puller = puller
	return store
}

type DBBatchEvent struct {
	batch         *pebble.Batch
	batchResolved *spanz.HashMap[uint64]
}

func (e *eventStore) getTableStat(span tablepb.Span) *tableState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	dispatcherID, ok := e.tables.Get(span)
	if !ok {
		return nil
	}
	ts, ok := e.spans[dispatcherID]
	if !ok {
		log.Panic("should not happen")
	}
	return ts
}

func (e *eventStore) batchCommitAndUpdateWatermark(ctx context.Context, batchCh chan *DBBatchEvent) {
	e.wg.Add(1)
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case batchEvent := <-batchCh:
			// do batch commit
			batch := batchEvent.batch
			if !batch.Empty() {
				if err := batch.Commit(pebble.NoSync); err != nil {
					log.Panic("failed to commit pebble batch", zap.Error(err))
				}
			}

			// update resolved ts after commit successfully
			batchResolved := batchEvent.batchResolved
			batchResolved.Range(func(span tablepb.Span, resolved uint64) bool {
				tableState := e.getTableStat(span)
				if tableState == nil {
					log.Debug("Table is removed, skip updating resolved")
					return true
				}
				tableState.notifier(resolved)
				return true
			})
		}
	}
}

const (
	batchCommitSize     int = 16 * 1024 * 1024
	batchCommitInterval     = 20 * time.Millisecond
)

func (e *eventStore) handleEvents(ctx context.Context, db *pebble.DB, inputCh <-chan eventWithTableID) {
	e.wg.Add(1)
	defer e.wg.Done()
	// 8 is an arbitrary number
	batchCh := make(chan *DBBatchEvent, 8)
	go e.batchCommitAndUpdateWatermark(ctx, batchCh)

	ticker := time.NewTicker(batchCommitInterval / 2)
	defer ticker.Stop()

	encodeItemAndBatch := func(batch *pebble.Batch, newResolved *spanz.HashMap[uint64], item eventWithTableID) {
		if item.raw.IsResolved() {
			newResolved.ReplaceOrInsert(item.span, item.raw.CRTs)
			return
		}
		key := EncodeKey(uint64(item.span.TableID), item.raw)
		value, err := json.Marshal(item.raw)
		if err != nil {
			log.Panic("failed to marshal event", zap.Error(err))
		}
		if err = batch.Set(key, value, pebble.NoSync); err != nil {
			log.Panic("failed to update pebble batch", zap.Error(err))
		}
	}

	// Batch item and commit until batch size is larger than batchCommitSize,
	// or the time since the last commit is larger than batchCommitInterval.
	// Only return false when the sorter is closed.
	doBatching := func() (*DBBatchEvent, bool) {
		batch := db.NewBatch()
		newResolved := spanz.NewHashMap[uint64]()
		startToBatch := time.Now()
		for {
			select {
			case item := <-inputCh:
				encodeItemAndBatch(batch, newResolved, item)
				if len(batch.Repr()) >= batchCommitSize {
					return &DBBatchEvent{batch, newResolved}, true
				}
			case <-ctx.Done():
				return nil, false
			case <-ticker.C:
				if time.Since(startToBatch) >= batchCommitInterval {
					return &DBBatchEvent{batch, newResolved}, true
				}
			}
		}
	}

	for {
		batchEvent, ok := doBatching()
		if !ok {
			return
		}
		select {
		case <-ctx.Done():
			return
		case batchCh <- batchEvent:
		}
	}
}

func (e *eventStore) writeEvent(span tablepb.Span, raw *common.RawKVEntry) {
	tableState := e.getTableStat(span)
	if tableState == nil {
		log.Panic("should not happen")
		return
	}
	if !raw.IsResolved() {
		tableState.observer(raw)
	}
	tableState.ch <- eventWithTableID{span: span, raw: raw}
}

func (e *eventStore) deleteEvents(span tablepb.Span, startCommitTS uint64, endCommitTS uint64) error {
	dbIndex := spanz.HashTableSpan(span, len(e.dbs))
	db := e.dbs[dbIndex]
	start := EncodeTsKey(uint64(span.TableID), startCommitTS)
	end := EncodeTsKey(uint64(span.TableID), endCommitTS)

	return db.DeleteRange(start, end, pebble.NoSync)
}

func (e *eventStore) RegisterDispatcher(dispatcherID common.DispatcherID, span tablepb.Span, startTS common.Ts, observer EventObserver, notifier WatermarkNotifier) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tables.ReplaceOrInsert(span, dispatcherID)
	tableState := &tableState{
		span:     span,
		observer: observer,
		notifier: notifier,
	}
	chIndex := spanz.HashTableSpan(span, len(e.channels))
	tableState.ch = e.channels[chIndex]
	tableState.watermark.Store(uint64(startTS))
	e.spans[dispatcherID] = tableState
	e.puller.Subscribe(span, startTS)
	return nil
}

func (e *eventStore) UpdateDispatcherSendTS(dispatcherID common.DispatcherID, sendTS uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if tableStat, ok := e.spans[dispatcherID]; ok {
		for {
			currentWatermark := tableStat.watermark.Load()
			if sendTS <= currentWatermark {
				return nil
			}
			if tableStat.watermark.CompareAndSwap(currentWatermark, sendTS) {
				e.gcManager.addGCItem(tableStat.span, currentWatermark, sendTS)
				return nil
			}
		}
	}
	return nil
}

func (e *eventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if tableStat, ok := e.spans[dispatcherID]; ok {
		e.puller.Unsubscribe(tableStat.span)
		e.tables.Delete(tableStat.span)
		delete(e.spans, dispatcherID)
	}
	return nil
}

func (e *eventStore) GetIterator(span tablepb.Span, startCommitTS uint64, endCommitTS uint64) (EventIterator, error) {
	// do some check
	tableStat := e.getTableStat(span)
	if tableStat == nil || tableStat.watermark.Load() > uint64(startCommitTS) {
		log.Panic("should not happen")
	}
	dbIndex := spanz.HashTableSpan(span, len(e.channels))
	db := e.dbs[dbIndex]
	// TODO: respect key range in span
	start := EncodeTsKey(uint64(span.TableID), uint64(startCommitTS), 0)
	end := EncodeTsKey(uint64(span.TableID), uint64(endCommitTS), 0)
	// TODO: use TableFilter/UseL6Filters in IterOptions
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	iter.First()

	return &eventStoreIter{
		innerIter:    iter,
		prevStartTS:  0,
		prevCommitTS: 0,
	}, nil
}

type eventStoreIter struct {
	innerIter    *pebble.Iterator
	prevStartTS  uint64
	prevCommitTS uint64
}

func (iter *eventStoreIter) Next() ([]byte, bool, error) {
	if iter.innerIter == nil {
		log.Panic("iter is nil")
	}

	if !iter.innerIter.Valid() {
		return nil, false, nil
	}

	key := iter.innerIter.Key()
	value := iter.innerIter.Value()
	_, startTS, commitTS := DecodeKey(key)

	isNewTxn := false
	if iter.prevCommitTS != 0 && iter.prevStartTS != 0 && (startTS != iter.prevStartTS || commitTS != iter.prevCommitTS) {
		isNewTxn = true
	}
	iter.prevCommitTS = commitTS
	iter.prevStartTS = startTS
	return value, isNewTxn, nil
}

func (iter *eventStoreIter) Close() error {
	if iter.innerIter != nil {
		return iter.innerIter.Close()
	}
	return nil
}