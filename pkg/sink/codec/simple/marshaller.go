// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	_ "embed"
	"encoding/json"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	ticommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/model"
)

//go:embed message.json
var avroSchemaBytes []byte

type marshaller interface {
	// MarshalCheckpoint marshals the checkpoint ts into bytes.
	MarshalCheckpoint(ts uint64) ([]byte, error)

	// MarshalDDLEvent marshals the DDL event into bytes.
	MarshalDDLEvent(event *model.DDLEvent) ([]byte, error)

	// MarshalRowChangedEvent marshals the row changed event into bytes.
	MarshalRowChangedEvent(event *common.RowChangedEvent,
		handleKeyOnly bool, claimCheckFileName string) ([]byte, error)

	// Unmarshal the bytes into the given value.
	Unmarshal(data []byte, v any) error
}

func newMarshaller(config *ticommon.Config) (marshaller, error) {
	var (
		result marshaller
		err    error
	)
	switch config.EncodingFormat {
	case ticommon.EncodingFormatJSON:
		result = newJSONMarshaller(config)
	case ticommon.EncodingFormatAvro:
		result, err = newAvroMarshaller(config, string(avroSchemaBytes))
	}
	return result, errors.Trace(err)
}

type JSONMarshaller struct {
	config *ticommon.Config
}

func newJSONMarshaller(config *ticommon.Config) *JSONMarshaller {
	return &JSONMarshaller{
		config: config,
	}
}

// MarshalCheckpoint implement the marshaller interface
func (m *JSONMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	msg := newResolvedMessage(ts)
	result, err := json.Marshal(msg)
	return result, errors.WrapError(errors.ErrEncodeFailed, err)
}

// MarshalDDLEvent implement the marshaller interface
func (m *JSONMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var msg *message
	if event.IsBootstrap {
		msg = newBootstrapMessage(event.TableInfo)
	} else {
		msg = newDDLMessage(event)
	}
	value, err := json.Marshal(msg)
	return value, errors.WrapError(errors.ErrEncodeFailed, err)
}

// MarshalRowChangedEvent implement the marshaller interface
func (m *JSONMarshaller) MarshalRowChangedEvent(
	event *common.RowChangedEvent,
	handleKeyOnly bool, claimCheckFileName string,
) ([]byte, error) {
	msg := m.newDMLMessage(event, handleKeyOnly, claimCheckFileName)
	value, err := json.Marshal(msg)
	return value, errors.WrapError(errors.ErrEncodeFailed, err)
}

// Unmarshal implement the marshaller interface
func (m *JSONMarshaller) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type avroMarshaller struct {
	codec  *goavro.Codec
	config *ticommon.Config
}

func newAvroMarshaller(config *ticommon.Config, schema string) (*avroMarshaller, error) {
	codec, err := goavro.NewCodec(schema)
	return &avroMarshaller{
		codec:  codec,
		config: config,
	}, errors.Trace(err)
}

// MarshalCheckpoint implement the marshaller interface
func (m *avroMarshaller) MarshalCheckpoint(ts uint64) ([]byte, error) {
	msg := newResolvedMessageMap(ts)
	result, err := m.codec.BinaryFromNative(nil, msg)
	return result, errors.WrapError(errors.ErrEncodeFailed, err)
}

// MarshalDDLEvent implement the marshaller interface
func (m *avroMarshaller) MarshalDDLEvent(event *model.DDLEvent) ([]byte, error) {
	var msg map[string]interface{}
	if event.IsBootstrap {
		msg = newBootstrapMessageMap(event.TableInfo)
	} else {
		msg = newDDLMessageMap(event)
	}
	value, err := m.codec.BinaryFromNative(nil, msg)
	return value, errors.WrapError(errors.ErrEncodeFailed, err)
}

// MarshalRowChangedEvent implement the marshaller interface
func (m *avroMarshaller) MarshalRowChangedEvent(
	event *common.RowChangedEvent,
	handleKeyOnly bool, claimCheckFileName string,
) ([]byte, error) {
	msg := m.newDMLMessageMap(event, handleKeyOnly, claimCheckFileName)
	value, err := m.codec.BinaryFromNative(nil, msg)
	recycleMap(msg)
	return value, errors.WrapError(errors.ErrEncodeFailed, err)
}

// Unmarshal implement the marshaller interface
func (m *avroMarshaller) Unmarshal(data []byte, v any) error {
	native, _, err := m.codec.NativeFromBinary(data)
	newMessageFromAvroNative(native, v.(*message))
	return errors.Trace(err)
}
