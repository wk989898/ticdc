// Copyright 2025 PingCAP, Inc.
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

package worker

import (
	"context"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	ticonfig "github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/encoder"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tidb/br/pkg/utils"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
)

type KafkaComponent struct {
	EncoderGroup   codec.EncoderGroup
	Encoder        encoder.EventEncoder
	ColumnSelector *columnselector.ColumnSelectors
	EventRouter    *eventrouter.EventRouter
	TopicManager   topicmanager.TopicManager
	AdminClient    kafka.ClusterAdminClient
	Factory        kafka.Factory
}

func getKafkaSinkComponentWithFactory(ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *ticonfig.SinkConfig,
	factoryCreator kafka.FactoryCreator,
) (KafkaComponent, ticonfig.Protocol, error) {
	kafkaComponent := KafkaComponent{}
	protocol, err := helper.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return kafkaComponent, ticonfig.ProtocolUnknown, errors.Trace(err)
	}
	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}
	scheme := sink.GetScheme(sinkURI)

	options := kafka.NewOptions()
	if err := options.Apply(changefeedID, sinkURI, sinkConfig); err != nil {
		return kafkaComponent, protocol, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	kafkaComponent.Factory, err = factoryCreator(options, changefeedID)
	if err != nil {
		return kafkaComponent, protocol, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	kafkaComponent.AdminClient, err = kafkaComponent.Factory.AdminClient(ctx)
	if err != nil {
		return kafkaComponent, protocol, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && kafkaComponent.AdminClient != nil {
			kafkaComponent.AdminClient.Close()
		}
	}()

	// adjust the option configuration before creating the kafka client
	if err = kafka.AdjustOptions(ctx, kafkaComponent.AdminClient, options, topic); err != nil {
		return kafkaComponent, protocol, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}

	kafkaComponent.TopicManager, err = topicmanager.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		kafkaComponent.AdminClient,
	)

	kafkaComponent.EventRouter, err = eventrouter.NewEventRouter(sinkConfig, protocol, topic, scheme)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.ColumnSelector, err = columnselector.NewColumnSelectors(sinkConfig)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, options.MaxMessageBytes)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.EncoderGroup = codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)

	kafkaComponent.Encoder, err = codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}
	return kafkaComponent, protocol, nil
}

func GetKafkaSinkComponent(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *ticonfig.SinkConfig,
) (KafkaComponent, ticonfig.Protocol, error) {
	factoryCreator := kafka.NewFactory
	return getKafkaSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, factoryCreator)
}

func GetKafkaSinkComponentForTest(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *ticonfig.SinkConfig,
) (KafkaComponent, ticonfig.Protocol, error) {
	return getKafkaSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, kafka.NewMockFactory)
}
