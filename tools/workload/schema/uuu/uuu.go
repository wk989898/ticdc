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

package uuu

import (
	"fmt"
	mrand "math/rand"
	"strings"
	"sync"

	"workload/schema"
)

const createDataTableFormat = `
CREATE TABLE IF NOT EXISTS Data%d (
    model_id bigint(20) unsigned NOT NULL,
    object_id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    object_value longblob,
    version int(11) unsigned NOT NULL,
    PRIMARY KEY(object_id)
);
`

const createIndexTableFormat = `
CREATE TABLE IF NOT EXISTS index_Data%d (
    object_id bigint(20) unsigned NOT NULL,
    reference_id bigint(20) DEFAULT NULL,
    guid varbinary(767) DEFAULT NULL,
    version int(11) unsigned NOT NULL,
    INDEX IndexOnGuid(guid, object_id),
    INDEX IndexOnReferenceId(reference_id, object_id),
    PRIMARY KEY(object_id)
);
`

type UUUWorkload struct{}

func NewUUUWorkload() schema.Workload {
	return &UUUWorkload{}
}

// BuildCreateTableStatement returns the create-table sql for both Data and index_Data tables
func (c *UUUWorkload) BuildCreateTableStatement(n int) string {
	if n%2 == 0 {
		return fmt.Sprintf(createDataTableFormat, n)
	}
	return fmt.Sprintf(createIndexTableFormat, n)
}

func (c *UUUWorkload) BuildInsertSql(tableN int, batchSize int) string {
	panic("unimplemented")
}

// BuildInsertSql returns two insert statements for Data and index_Data tables
func (c *UUUWorkload) BuildInsertSqlWithValues(tableN int, batchSize int) (string, []interface{}) {
	var sql string
	values := make([]interface{}, 0, batchSize*4) // 预分配空间：每行4个值

	if tableN%2 == 0 {
		// Data table insert
		sql = fmt.Sprintf("INSERT INTO Data%d (model_id, object_id, object_value, version) VALUES ", tableN)
		placeholders := make([]string, batchSize)
		for r := 0; r < batchSize; r++ {
			n := mrand.Int63()
			values = append(values, n, n, generateStringForData(), 1)
			placeholders[r] = "(?,?,?,?)"
		}
		return sql + strings.Join(placeholders, ","), values
	} else {
		// Index table insert
		sql = fmt.Sprintf("INSERT INTO index_Data%d (object_id, reference_id, guid, version) VALUES ", tableN)
		placeholders := make([]string, batchSize)

		for r := 0; r < batchSize; r++ {
			n := mrand.Int63()
			values = append(values, n, n, generateStringForIndex(), 1)
			placeholders[r] = "(?,?,?,?)"
		}
		return sql + strings.Join(placeholders, ","), values
	}
}

func (c *UUUWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	panic("unimplemented")
}

var (
	preGeneratedStringForData string
	columnLenForData          = 1024
	onceForData               sync.Once
)

func generateStringForData() string {
	onceForData.Do(func() {
		builder := strings.Builder{}
		for i := 0; i < columnLenForData; i++ {
			builder.WriteString(fmt.Sprintf("%d", i))
		}
		preGeneratedStringForData = builder.String()
	})
	return preGeneratedStringForData
}

var (
	preGeneratedStringForIndex string
	columnLenForIndex          = 256
	onceForIndex               sync.Once
)

func generateStringForIndex() string {
	onceForIndex.Do(func() {
		builder := strings.Builder{}
		for i := 0; i < columnLenForIndex; i++ {
			builder.WriteString(fmt.Sprintf("%d", i))
		}
		preGeneratedStringForIndex = builder.String()
	})
	return preGeneratedStringForIndex
}
