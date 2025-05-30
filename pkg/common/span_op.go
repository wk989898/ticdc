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

package common

import (
	"bytes"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
)

const (
	// JobTableID is the id of `tidb_ddl_job`.
	JobTableID = ddl.JobTableID
	// JobHistoryID is the id of `tidb_ddl_history`
	JobHistoryID = ddl.HistoryTableID
)

// TableIDToComparableSpan converts a TableID to a Span whose
// StartKey and EndKey are encoded in Comparable format.
func TableIDToComparableSpan(tableID int64) heartbeatpb.TableSpan {
	startKey, endKey := GetTableRange(tableID)
	return heartbeatpb.TableSpan{
		TableID:  tableID,
		StartKey: ToComparableKey(startKey),
		EndKey:   ToComparableKey(endKey),
	}
}

// TableIDToComparableRange returns a range of a table,
// start and end are encoded in Comparable format.
func TableIDToComparableRange(tableID int64) (start, end heartbeatpb.TableSpan) {
	tableSpan := TableIDToComparableSpan(tableID)
	start = tableSpan
	start.EndKey = nil
	end = tableSpan
	end.StartKey = tableSpan.EndKey
	end.EndKey = nil
	return
}

func IsCompleteSpan(tableSpan *heartbeatpb.TableSpan) bool {
	startKey, endKey := GetTableRange(tableSpan.TableID)
	if StartCompare(ToComparableKey(startKey), tableSpan.StartKey) == 0 && EndCompare(ToComparableKey(endKey), tableSpan.EndKey) == 0 {
		return true
	}
	return false
}

// UpperBoundKey represents the maximum value.
var UpperBoundKey = []byte{255, 255, 255, 255, 255}

// HackTableSpan will set End as UpperBoundKey if End is Nil.
func HackTableSpan(span heartbeatpb.TableSpan) heartbeatpb.TableSpan {
	if span.StartKey == nil {
		span.StartKey = []byte{}
	}

	if span.EndKey == nil {
		span.EndKey = UpperBoundKey
	}
	return span
}

// GetTableRange returns the span to watch for the specified table
// Note that returned keys are not in memcomparable format.
func GetTableRange(tableID int64) (startKey, endKey []byte) {
	tablePrefix := tablecodec.GenTablePrefix(tableID)
	sep := byte('_')
	recordMarker := byte('r')

	var start, end kv.Key
	// ignore index keys.
	start = append(tablePrefix, sep, recordMarker)
	end = append(tablePrefix, sep, recordMarker+1)
	return start, end
}

// StartCompare compares two start keys.
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func StartCompare(lhs []byte, rhs []byte) int {
	if len(lhs) == 0 && len(rhs) == 0 {
		return 0
	}

	// Nil means Negative infinity.
	// It's different with EndCompare.
	if len(lhs) == 0 {
		return -1
	}

	if len(rhs) == 0 {
		return 1
	}

	return bytes.Compare(lhs, rhs)
}

// EndCompare compares two end keys.
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func EndCompare(lhs []byte, rhs []byte) int {
	if len(lhs) == 0 && len(rhs) == 0 {
		return 0
	}

	// Nil means Positive infinity.
	// It's difference with StartCompare.
	if len(lhs) == 0 {
		return 1
	}

	if len(rhs) == 0 {
		return -1
	}

	return bytes.Compare(lhs, rhs)
}

// GetIntersectSpan return the intersect part of lhs and rhs span
func GetIntersectSpan(lhs, rhs heartbeatpb.TableSpan) heartbeatpb.TableSpan {
	if len(lhs.StartKey) != 0 && EndCompare(lhs.StartKey, rhs.EndKey) >= 0 ||
		len(rhs.StartKey) != 0 && EndCompare(rhs.StartKey, lhs.EndKey) >= 0 {
		return heartbeatpb.TableSpan{
			StartKey: nil,
			EndKey:   nil,
		}
	}

	start := lhs.StartKey

	if StartCompare(rhs.StartKey, start) > 0 {
		start = rhs.StartKey
	}

	end := lhs.EndKey

	if EndCompare(rhs.EndKey, end) < 0 {
		end = rhs.EndKey
	}

	return heartbeatpb.TableSpan{
		StartKey: start,
		EndKey:   end,
	}
}

// IsSubSpan returns true if the sub span is parents spans
func IsSubSpan(sub heartbeatpb.TableSpan, parents ...heartbeatpb.TableSpan) bool {
	if bytes.Compare(sub.StartKey, sub.EndKey) >= 0 {
		log.Panic("the sub span is invalid", zap.Reflect("subSpan", sub))
	}
	for _, parent := range parents {
		if StartCompare(parent.StartKey, sub.StartKey) <= 0 &&
			EndCompare(sub.EndKey, parent.EndKey) <= 0 {
			return true
		}
	}
	return false
}

// IsEmptySpan returns true if the span is empty.
// TODO: check whether need span.StartKey >= span.EndKey
func IsEmptySpan(span heartbeatpb.TableSpan) bool {
	return len(span.StartKey) == 0 && len(span.EndKey) == 0
}

// ToSpan returns a span, keys are encoded in memcomparable format.
// See: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
func ToSpan(startKey, endKey []byte) heartbeatpb.TableSpan {
	return heartbeatpb.TableSpan{
		StartKey: ToComparableKey(startKey),
		EndKey:   ToComparableKey(endKey),
	}
}

// ToComparableKey returns a memcomparable key.
// See: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
func ToComparableKey(key []byte) []byte {
	return codec.EncodeBytes(nil, key)
}
