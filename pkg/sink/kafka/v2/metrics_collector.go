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

package v2

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"go.uber.org/zap"
)

// MetricsCollector is the kafka metrics collector based on kafka-go library.
type MetricsCollector struct {
	changefeedID common.ChangeFeedID
	writer       Writer
}

// NewMetricsCollector return a kafka metrics collector
func NewMetricsCollector(
	changefeedID common.ChangeFeedID,
	writer Writer,
) *MetricsCollector {
	return &MetricsCollector{
		changefeedID: changefeedID,
		writer:       writer,
	}
}

// Run implement the MetricsCollector interface
func (m *MetricsCollector) Run(ctx context.Context) {
	ticker := time.NewTicker(kafka.RefreshMetricsInterval)
	defer func() {
		ticker.Stop()
		m.cleanupMetrics()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("Kafka metrics collector stopped",
				zap.String("namespace", m.changefeedID.Namespace()),
				zap.String("changefeed", m.changefeedID.Name()))
			return
		case <-ticker.C:
			m.collectMetrics()
		}
	}
}

func (m *MetricsCollector) collectMetrics() {
	statistics := m.writer.Stats()

	// batch related metrics
	kafka.BatchDurationGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name()).
		Set(statistics.BatchQueueTime.Avg.Seconds())
	kafka.BatchMessageCountGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name()).
		Set(float64(statistics.BatchSize.Avg))
	kafka.BatchSizeGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name()).
		Set(float64(statistics.BatchBytes.Avg))

	// send request related metrics.
	// metrics is collected each 5 seconds, divide by 5 to get per seconds average.
	// since kafka-go does not support per broker metrics, so we add `v2` as the broker ID.
	kafka.RequestRateGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), "v2").
		Set(float64(statistics.Writes / 5))
	kafka.RequestLatencyGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), "v2", "avg").
		Set(statistics.WriteTime.Avg.Seconds())
	kafka.OutgoingByteRateGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), "v2").
		Set(float64(statistics.Bytes / 5))

	kafka.ClientRetryGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name()).
		Set(float64(statistics.Retries))
	kafka.ClientErrorGauge.WithLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name()).
		Set(float64(statistics.Errors))
}

func (m *MetricsCollector) cleanupMetrics() {
	kafka.BatchDurationGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name())
	kafka.BatchMessageCountGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name())
	kafka.BatchSizeGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name())

	kafka.RequestRateGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), "v2")
	kafka.RequestLatencyGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), "v2", "avg")
	kafka.OutgoingByteRateGauge.
		DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name(), "v2")

	kafka.ClientRetryGauge.DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name())
	kafka.ClientErrorGauge.DeleteLabelValues(m.changefeedID.Namespace(), m.changefeedID.Name())
}
