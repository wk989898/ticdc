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

package causality

import (
	"sync/atomic"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
)

const (
	// BlockStrategyWaitAvailable means the cache will block until there is an available slot.
	BlockStrategyWaitAvailable BlockStrategy = "waitAvailable"
	// BlockStrategyWaitEmpty means the cache will block until all cached txns are consumed.
	BlockStrategyWaitEmpty = "waitEmpty"
	// TODO: maybe we can implement a strategy that can automatically adapt to different scenarios
)

// BlockStrategy is the strategy to handle the situation when the cache is full.
type BlockStrategy string

// TxnCacheOption is the option for creating a cache for resolved txns.
type TxnCacheOption struct {
	// Count controls the number of caches, txns in different caches could be executed concurrently.
	Count int
	// Size controls the max number of txns a cache can hold.
	Size int
	// BlockStrategy controls the strategy when the cache is full.
	BlockStrategy BlockStrategy
}

// In current implementation, the conflict detector will push txn to the txnCache.
type txnCache interface {
	// add adds a event to the Cache.
	add(txn *commonEvent.DMLEvent) bool
	// out returns a unlimited channel to receive events which are ready to be executed.
	out() *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]
}

func newTxnCache(opt TxnCacheOption) txnCache {
	if opt.Size <= 0 {
		log.Panic("WorkerOption.CacheSize should be greater than 0, please report a bug")
	}

	switch opt.BlockStrategy {
	case BlockStrategyWaitAvailable:
		return &boundedTxnCache{ch: chann.NewUnlimitedChannel[*commonEvent.DMLEvent, any](nil, nil), upperSize: opt.Size}
	case BlockStrategyWaitEmpty:
		return &boundedTxnCacheWithBlock{ch: chann.NewUnlimitedChannel[*commonEvent.DMLEvent, any](nil, nil), upperSize: opt.Size}
	default:
		return nil
	}
}

// boundedTxnCache is a cache which has a limit on the number of txns it can hold.
//
//nolint:unused
type boundedTxnCache struct {
	ch        *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]
	upperSize int
}

//nolint:unused
func (w *boundedTxnCache) add(txn *commonEvent.DMLEvent) bool {
	if w.ch.Len() > w.upperSize {
		return false
	}
	w.ch.Push(txn)
	return true
}

//nolint:unused
func (w *boundedTxnCache) out() *chann.UnlimitedChannel[*commonEvent.DMLEvent, any] {
	return w.ch
}

// boundedTxnCacheWithBlock is a special boundedWorker. Once the cache
// is full, it will block until all cached txns are consumed.
type boundedTxnCacheWithBlock struct {
	ch *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]
	//nolint:unused
	isBlocked atomic.Bool
	upperSize int
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) add(txn *commonEvent.DMLEvent) bool {
	if w.isBlocked.Load() && w.ch.Len() <= 0 {
		w.isBlocked.Store(false)
	}

	if !w.isBlocked.Load() {
		if w.ch.Len() > w.upperSize {
			w.isBlocked.CompareAndSwap(false, true)
			return false
		}
		w.ch.Push(txn)
		return true
	}
	return false
}

//nolint:unused
func (w *boundedTxnCacheWithBlock) out() *chann.UnlimitedChannel[*commonEvent.DMLEvent, any] {
	return w.ch
}
