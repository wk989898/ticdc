#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=10

# This test mainly verifies kv client force reconnect can work
# Trigger force reconnect by failpoint injection
function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="kv-client-stream-reconnect-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR oauth
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac

	# this will be triggered every 5s in logpuller
	export GO_FAILPOINTS='github.com/pingcap/ticdc/logservice/logpuller/InjectForceReconnect=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	if [ "$SINK_TYPE" == "pulsar" ]; then
		cat <<EOF >>$WORK_DIR/pulsar_test.toml
          [sink.pulsar-config.oauth2]
          oauth2-issuer-url="http://localhost:9096"
          oauth2-audience="cdc-api-uri"
          oauth2-client-id="1234"
          oauth2-private-key="${WORK_DIR}/credential.json"
EOF
	else
		echo "" >$WORK_DIR/pulsar_test.toml
	fi
	changefeed_id=$(cdc cli changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" --config $WORK_DIR/pulsar_test.toml 2>&1 | tail -n2 | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --oauth2-private-key ${WORK_DIR}/credential.json --oauth2-issuer-url "http://localhost:9096" -- oauth2-client-id "1234" ;;
	esac

	run_sql "CREATE DATABASE kv_client_stream_reconnect;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	for i in $(seq $TABLE_COUNT); do
		run_sql "create table kv_client_stream_reconnect.t$i (id int primary key auto_increment, a int default 10);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	for i in $(seq 60); do
		tbl="t$((1 + $RANDOM % $TABLE_COUNT))"
		run_sql "insert into kv_client_stream_reconnect.$tbl values (),(),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		sleep 1
	done

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
