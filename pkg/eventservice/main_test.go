// Copyright 2022 PingCAP, Inc.
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

package eventservice

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/leakutil"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/pingcap/tiflow/pkg/workerpool.(*worker).run"),
		goleak.IgnoreTopFunction("sync.runtime_Semacquire"),
		goleak.IgnoreAnyFunction("github.com/godbus/dbus.(*Conn).Auth"),
		goleak.IgnoreCurrent(),
	}

	leakutil.SetUpLeakTest(m, opts...)
}
