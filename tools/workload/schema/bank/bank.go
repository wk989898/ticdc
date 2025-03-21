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

package bank

import (
	"bytes"
	"fmt"
	"math/rand"

	"workload/schema"
)

const createBankTable = `
create table if not exists bank%d
(
id    BIGINT NOT NULL,                    
col1  VARCHAR(5),                    
col2  DATETIME,                    
col3  DATETIME,                    
col4  NUMERIC(3,0),                    
col5  CHAR(25),                    
col6  CHAR(10),                    
col7  CHAR(6),                    
col8  NUMERIC(10,0),                    
col9  CHAR(5),                    
col10 NUMERIC(4,0),                    
col11 NUMERIC(4,0),                    
col12 CHAR(5),                    
col13 CHAR(8),                    
col14 CHAR(9),                    
col15 CHAR(9),                    
col16 CHAR(9),                    
col17 CHAR(9),                    
col18 CHAR(3),                    
col19 CHAR(1),                    
col20 VARCHAR(10),                    
col21 CHAR(1),                    
col22 NUMERIC(19,0),                    
col23 CHAR(1),                    
col24 VARCHAR(8),                    
col25 VARCHAR(240),                    
col26 CHAR(1),                    
col27 CHAR(1),                    
col28 NUMERIC(3,0),                    
col29 CHAR(6),                    
col30 VARCHAR(10),                    
col31 CHAR(9),                    
col32 NUMERIC(3,0),                    
col33 VARCHAR(6),                    
col34 VARCHAR(8),                    
col35 VARCHAR(10),                    
col36 VARCHAR(5),                    
col37 VARCHAR(5),                    
col38 CHAR(1),                    
col39 VARCHAR(32),                    
col40 CHAR(23),                    
col41 VARCHAR(5),                    
col42 CHAR(1),                    
col43 VARCHAR(240),                    
col44 CHAR(18),                    
col45 CHAR(4),                    
col46 CHAR(2),                    
col47 CHAR(1),                    
col48 NUMERIC(2,0),                    
col49 VARCHAR(35),                    
col50 CHAR(8),                    
col51 CHAR(8),                    
col52 CHAR(8),                    
col53 VARCHAR(240),                    
col54 CHAR(8),                    
col55 CHAR(23),                    
col56 CHAR(2),                    
col57 VARCHAR(10),                    
col58 DATETIME,                    
col59 DATETIME,                    
col60 DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3),                    
col61 VARCHAR(30) DEFAULT 'Z',
PRIMARY KEY (id)
);
`

type BankWorkload struct{}

func NewBankWorkload() schema.Workload {
	return &BankWorkload{}
}

func (c *BankWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createBankTable, n)
}

func (c *BankWorkload) BuildInsertSql(tableN int, batchSize int) string {
	n := rand.Int63()
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(`
insert into bank%d (id, 
col1,
col2,
col3,
col4,
col5,
col6,
col7,
col8,
col9,
col10,
col11,
col12,
col13,
col14,
col15,
col16,
col17,
col18,
col19,
col20,
col21,
col22,
col23,
col24,
col25,
col26,
col27,
col28,
col29,
col30,
col31,
col32,
col33,
col34,
col35,
col36,
col37,
col38,
col39,
col40,
col41,
col42,
col43,
col44,
col45,
col46,
col47,
col48,
col49,
col50,
col51,
col52,
col53,
col54,
col55,
col56,
col57,
col58,
col59,
col60,
col61
)
values(%d, 
'abcde', 
'2019-03-05 01:53:56', 
'2019-03-05 01:53:56',
100,
'abcdefghijklmnopsrstuvwxy', 
'1234567890',
'123456', 
10000,
'12345',
1000,
1000,
'12345',
'12345678',
'123456789',
'123456789',
'123456789',
'123456789',
'123',
'1',
'1234567890',
'1',
1111111,
'1',
'12345678',
'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
'1',
'1',
100,
'123456',
'1234567890',
'123456789',
100,
'123456',
'12345678',
'1234567890',
'12345',
'12345',
'1',
'12345678901234567890123456789012',
'12345678901234567890123',
'12345',
'1',
'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
'123456789012345678',
'1234',
'12',
'1',
10,
'12345678901234567890123456789012345',
'12345678',
'12345678',
'12345678',
'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
'12345678',
'12345678901234567890123',
'12',
'1234567890',
'2019-03-05 01:53:56',
'2019-03-05 01:53:56',
'2019-03-05 01:53:56',
'123456789012345678901234567890'
)
`, tableN, n))

	for r := 1; r < batchSize; r++ {
		n = rand.Int63()
		buf.WriteString(fmt.Sprintf(
			`,
(%d, 
'abcde', 
'2019-03-05 01:53:56', 
'2019-03-05 01:53:56',
100,
'abcdefghijklmnopsrstuvwxy', 
'1234567890',
'123456', 
10000,
'12345',
1000,
1000,
'12345',
'12345678',
'123456789',
'123456789',
'123456789',
'123456789',
'123',
'1',
'1234567890',
'1',
1111111,
'1',
'12345678',
'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
'1',
'1',
100,
'123456',
'1234567890',
'123456789',
100,
'123456',
'12345678',
'1234567890',
'12345',
'12345',
'1',
'12345678901234567890123456789012',
'12345678901234567890123',
'12345',
'1',
'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
'123456789012345678',
'1234',
'12',
'1',
10,
'12345678901234567890123456789012345',
'12345678',
'12345678',
'12345678',
'123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890',
'12345678',
'12345678901234567890123',
'12',
'1234567890',
'2019-03-05 01:53:56',
'2019-03-05 01:53:56',
'2019-03-05 01:53:56',
'123456789012345678901234567890'
)
`, n))
	}
	return buf.String()
}

func (c *BankWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	panic("unimplemented")
}
