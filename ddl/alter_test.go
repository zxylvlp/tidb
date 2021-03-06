// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/util/types"
)

func (s *testDDLSuite) TestAlterSpecification(c *C) {
	tbl := []*AlterSpecification{
		{
			Action: AlterTableOpt,
		},
		{
			Action: AlterDropColumn,
			Name:   "c1",
		},
		{Action: AlterDropPrimaryKey},
		{Action: AlterDropForeignKey,
			Name: "c"},
		{Action: AlterDropIndex,
			Name: "index_c"},
		{Action: AlterAddConstr,
			Constraint: nil},
		{Action: AlterAddConstr,
			Constraint: &coldef.TableConstraint{
				Tp: coldef.ConstrPrimaryKey,
				Keys: []*coldef.IndexColName{
					{
						ColumnName: "a",
						Length:     10,
					},
				},
			}},
		{Action: AlterAddColumn,
			Column: &coldef.ColumnDef{
				Name: "c",
				Tp:   types.NewFieldType(mysql.TypeLong),
			},
			Position: &ColumnPosition{}},
		{Action: AlterAddColumn,
			Column: &coldef.ColumnDef{
				Name: "c",
				Tp:   types.NewFieldType(mysql.TypeLong),
			},
			Position: &ColumnPosition{Type: ColumnPositionFirst}},
		{Action: AlterAddColumn,
			Column: &coldef.ColumnDef{
				Name: "c",
				Tp:   types.NewFieldType(mysql.TypeLong),
			},
			Position: &ColumnPosition{Type: ColumnPositionAfter,
				RelativeColumn: "c"}},

		// Invalid action returns empty string
		{Action: -1},
	}

	for _, spec := range tbl {
		c.Assert(len(spec.String()), GreaterEqual, 0)
	}
}
