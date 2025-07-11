// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !codes

package core

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
)

func newLongType() types.FieldType {
	return *(types.NewFieldType(mysql.TypeLong))
}

func newStringType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	charset, collate := types.DefaultCharsetForType(mysql.TypeVarchar)
	ft.SetCharset(charset)
	ft.SetCollate(collate)
	return *ft
}

func newDateType() types.FieldType {
	ft := types.NewFieldType(mysql.TypeDate)
	return *ft
}

// MockSignedTable is only used for plan related tests.
func MockSignedTable() *model.TableInfo {
	// column: a, b, c, d, e, c_str, d_str, e_str, f, g, h, i_date
	// PK: a
	// indices: c_d_e, e, f, g, f_g, c_d_e_str, e_d_c_str_prefix
	indices := []*model.IndexInfo{
		{
			ID:   1,
			Name: ast.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
				{
					Name:   ast.NewCIStr("d"),
					Length: types.UnspecifiedLength,
					Offset: 3,
				},
				{
					Name:   ast.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			ID:   2,
			Name: ast.NewCIStr("x"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("e"),
					Length: types.UnspecifiedLength,
					Offset: 4,
				},
			},
			State:  model.StateWriteOnly,
			Unique: true,
		},
		{
			ID:   3,
			Name: ast.NewCIStr("f"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 8,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			ID:   4,
			Name: ast.NewCIStr("g"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   5,
			Name: ast.NewCIStr("f_g"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("f"),
					Length: types.UnspecifiedLength,
					Offset: 8,
				},
				{
					Name:   ast.NewCIStr("g"),
					Length: types.UnspecifiedLength,
					Offset: 9,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			ID:   6,
			Name: ast.NewCIStr("c_d_e_str"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("c_str"),
					Length: types.UnspecifiedLength,
					Offset: 5,
				},
				{
					Name:   ast.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   ast.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
			},
			State: model.StatePublic,
		},
		{
			ID:   7,
			Name: ast.NewCIStr("e_d_c_str_prefix"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("e_str"),
					Length: types.UnspecifiedLength,
					Offset: 7,
				},
				{
					Name:   ast.NewCIStr("d_str"),
					Length: types.UnspecifiedLength,
					Offset: 6,
				},
				{
					Name:   ast.NewCIStr("c_str"),
					Length: 10,
					Offset: 5,
				},
			},
			State: model.StatePublic,
		},
	}
	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      ast.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      ast.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    2,
		Name:      ast.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	col2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    3,
		Name:      ast.NewCIStr("d"),
		FieldType: newLongType(),
		ID:        4,
	}
	col3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    4,
		Name:      ast.NewCIStr("e"),
		FieldType: newLongType(),
		ID:        5,
	}
	colStr1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    5,
		Name:      ast.NewCIStr("c_str"),
		FieldType: newStringType(),
		ID:        6,
	}
	colStr2 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    6,
		Name:      ast.NewCIStr("d_str"),
		FieldType: newStringType(),
		ID:        7,
	}
	colStr3 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    7,
		Name:      ast.NewCIStr("e_str"),
		FieldType: newStringType(),
		ID:        8,
	}
	col4 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    8,
		Name:      ast.NewCIStr("f"),
		FieldType: newLongType(),
		ID:        9,
	}
	col5 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    9,
		Name:      ast.NewCIStr("g"),
		FieldType: newLongType(),
		ID:        10,
	}
	col6 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    10,
		Name:      ast.NewCIStr("h"),
		FieldType: newLongType(),
		ID:        11,
	}
	col7 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    11,
		Name:      ast.NewCIStr("i_date"),
		FieldType: newDateType(),
		ID:        12,
	}
	pkColumn.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	// Column 'b', 'c', 'd', 'f', 'g' is not null.
	col0.SetFlag(mysql.NotNullFlag)
	col1.SetFlag(mysql.NotNullFlag)
	col2.SetFlag(mysql.NotNullFlag)
	col4.SetFlag(mysql.NotNullFlag)
	col5.SetFlag(mysql.NotNullFlag)
	col6.SetFlag(mysql.NoDefaultValueFlag)
	table := &model.TableInfo{
		ID:         1,
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2, col3, colStr1, colStr2, colStr3, col4, col5, col6, col7},
		Indices:    indices,
		Name:       ast.NewCIStr("t"),
		PKIsHandle: true,
		State:      model.StatePublic,
	}
	return table
}

// MockUnsignedTable is only used for plan related tests.
func MockUnsignedTable() *model.TableInfo {
	// column: a, b, c
	// PK: a
	// indeices: b, b_c
	indices := []*model.IndexInfo{
		{
			Name: ast.NewCIStr("b"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
		{
			Name: ast.NewCIStr("b_c"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   ast.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
			},
			State: model.StatePublic,
		},
	}
	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      ast.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      ast.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    2,
		Name:      ast.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	pkColumn.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag)
	// Column 'b' is not null.
	col0.SetFlag(mysql.NotNullFlag)
	col1.SetFlag(mysql.UnsignedFlag)
	table := &model.TableInfo{
		ID:         2,
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1},
		Indices:    indices,
		Name:       ast.NewCIStr("t2"),
		PKIsHandle: true,
		State:      model.StatePublic,
	}
	return table
}

// MockNoPKTable is only used for plan related tests.
func MockNoPKTable() *model.TableInfo {
	// column: a, b
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      ast.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    2,
		Name:      ast.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        3,
	}
	// Column 'a', 'b' is not null.
	col0.SetFlag(mysql.NotNullFlag)
	col1.SetFlag(mysql.UnsignedFlag)
	table := &model.TableInfo{
		ID:         3,
		Columns:    []*model.ColumnInfo{col0, col1},
		Name:       ast.NewCIStr("t3"),
		PKIsHandle: true,
		State:      model.StatePublic,
	}
	return table
}

// MockView is only used for plan related tests.
func MockView() *model.TableInfo {
	selectStmt := "select b,c,d from t"
	col0 := &model.ColumnInfo{
		State:  model.StatePublic,
		Offset: 0,
		Name:   ast.NewCIStr("b"),
		ID:     1,
	}
	col1 := &model.ColumnInfo{
		State:  model.StatePublic,
		Offset: 1,
		Name:   ast.NewCIStr("c"),
		ID:     2,
	}
	col2 := &model.ColumnInfo{
		State:  model.StatePublic,
		Offset: 2,
		Name:   ast.NewCIStr("d"),
		ID:     3,
	}
	view := &model.ViewInfo{SelectStmt: selectStmt, Security: ast.SecurityDefiner, Definer: &auth.UserIdentity{Username: "root", Hostname: ""}, Cols: []ast.CIStr{col0.Name, col1.Name, col2.Name}}
	table := &model.TableInfo{
		ID:      4,
		Name:    ast.NewCIStr("v"),
		Columns: []*model.ColumnInfo{col0, col1, col2},
		View:    view,
		State:   model.StatePublic,
	}
	return table
}

// MockContext is only used for plan related tests.
func MockContext() *mock.Context {
	ctx := mock.NewContext()
	ctx.Store = &mock.Store{
		Client: &mock.Client{},
	}
	initStatsCtx := mock.NewContext()
	initStatsCtx.Store = &mock.Store{
		Client: &mock.Client{},
	}
	ctx.GetSessionVars().CurrentDB = "test"
	ctx.GetSessionVars().DivPrecisionIncrement = vardef.DefDivPrecisionIncrement
	do := domain.NewMockDomain()
	if err := do.CreateStatsHandle(context.Background(), initStatsCtx); err != nil {
		panic(fmt.Sprintf("create mock context panic: %+v", err))
	}
	ctx.BindDomainAndSchValidator(do, nil)
	return ctx
}

// MockPartitionInfoSchema mocks an info schema for partition table.
func MockPartitionInfoSchema(definitions []model.PartitionDefinition) infoschema.InfoSchema {
	tableInfo := MockSignedTable()
	cols := make([]*model.ColumnInfo, 0, len(tableInfo.Columns))
	cols = append(cols, tableInfo.Columns...)
	last := tableInfo.Columns[len(tableInfo.Columns)-1]
	cols = append(cols, &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    last.Offset + 1,
		Name:      ast.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &model.PartitionInfo{
		Type:        ast.PartitionTypeRange,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
	}
	tableInfo.Columns = cols
	tableInfo.Partition = partition
	is := infoschema.MockInfoSchema([]*model.TableInfo{tableInfo})
	return is
}

// MockRangePartitionTable mocks a range partition table for test
func MockRangePartitionTable() *model.TableInfo {
	definitions := []model.PartitionDefinition{
		{
			ID:       41,
			Name:     ast.NewCIStr("p1"),
			LessThan: []string{"16"},
		},
		{
			ID:       42,
			Name:     ast.NewCIStr("p2"),
			LessThan: []string{"32"},
		},
	}
	tableInfo := MockSignedTable()
	tableInfo.ID = 5
	tableInfo.Name = ast.NewCIStr("pt1")
	cols := make([]*model.ColumnInfo, 0, len(tableInfo.Columns))
	cols = append(cols, tableInfo.Columns...)
	last := tableInfo.Columns[len(tableInfo.Columns)-1]
	cols = append(cols, &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    last.Offset + 1,
		Name:      ast.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &model.PartitionInfo{
		Type:        ast.PartitionTypeRange,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
	}
	tableInfo.Columns = cols
	tableInfo.Partition = partition
	return tableInfo
}

// MockHashPartitionTable mocks a hash partition table for test
func MockHashPartitionTable() *model.TableInfo {
	definitions := []model.PartitionDefinition{
		{
			ID:   51,
			Name: ast.NewCIStr("p1"),
		},
		{
			ID:   52,
			Name: ast.NewCIStr("p2"),
		},
	}
	tableInfo := MockSignedTable()
	tableInfo.ID = 6
	tableInfo.Name = ast.NewCIStr("pt2")
	cols := make([]*model.ColumnInfo, 0, len(tableInfo.Columns))
	cols = append(cols, tableInfo.Columns...)
	last := tableInfo.Columns[len(tableInfo.Columns)-1]
	cols = append(cols, &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    last.Offset + 1,
		Name:      ast.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &model.PartitionInfo{
		Type:        ast.PartitionTypeHash,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
		Num:         2,
	}
	tableInfo.Columns = cols
	tableInfo.Partition = partition
	return tableInfo
}

// MockListPartitionTable mocks a list partition table for test
func MockListPartitionTable() *model.TableInfo {
	definitions := []model.PartitionDefinition{
		{
			ID:   61,
			Name: ast.NewCIStr("p1"),
			InValues: [][]string{
				{
					"1",
				},
			},
		},
		{
			ID:   62,
			Name: ast.NewCIStr("p2"),
			InValues: [][]string{
				{
					"2",
				},
			},
		},
	}
	tableInfo := MockSignedTable()
	tableInfo.ID = 7
	tableInfo.Name = ast.NewCIStr("pt3")
	cols := make([]*model.ColumnInfo, 0, len(tableInfo.Columns))
	cols = append(cols, tableInfo.Columns...)
	last := tableInfo.Columns[len(tableInfo.Columns)-1]
	cols = append(cols, &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    last.Offset + 1,
		Name:      ast.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &model.PartitionInfo{
		Type:        ast.PartitionTypeList,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
		Num:         2,
	}
	tableInfo.Columns = cols
	tableInfo.Partition = partition
	return tableInfo
}

// MockGlobalIndexHashPartitionTable mocks a hash partition table with global index for test
func MockGlobalIndexHashPartitionTable() *model.TableInfo {
	definitions := []model.PartitionDefinition{
		{
			ID:   51,
			Name: ast.NewCIStr("p1"),
		},
		{
			ID:   52,
			Name: ast.NewCIStr("p2"),
		},
	}
	tableInfo := MockSignedTable()
	tableInfo.Name = ast.NewCIStr("pt2_global_index")
	cols := make([]*model.ColumnInfo, 0, len(tableInfo.Columns))
	cols = append(cols, tableInfo.Columns...)
	last := tableInfo.Columns[len(tableInfo.Columns)-1]
	cols = append(cols, &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    last.Offset + 1,
		Name:      ast.NewCIStr("ptn"),
		FieldType: newLongType(),
		ID:        last.ID + 1,
	})
	partition := &model.PartitionInfo{
		Type:        ast.PartitionTypeHash,
		Expr:        "ptn",
		Enable:      true,
		Definitions: definitions,
		Num:         2,
	}
	tableInfo.Columns = cols
	tableInfo.Partition = partition
	// add a global index `b_global` and `b_c_global` and normal index `b` and `b_c`
	tableInfo.Indices = append(tableInfo.Indices, []*model.IndexInfo{
		{
			Name: ast.NewCIStr("b"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: ast.NewCIStr("b_global"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State:  model.StatePublic,
			Unique: true,
			Global: true,
		},
		{
			Name: ast.NewCIStr("b_c"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   ast.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
			},
			State: model.StatePublic,
		},
		{
			Name: ast.NewCIStr("b_c_global"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
				{
					Name:   ast.NewCIStr("c"),
					Length: types.UnspecifiedLength,
					Offset: 2,
				},
			},
			State:  model.StatePublic,
			Unique: true,
			Global: true,
		},
	}...)
	return tableInfo
}

// MockStateNoneColumnTable is only used for plan related tests.
func MockStateNoneColumnTable() *model.TableInfo {
	// column: a, b
	// PK: a
	// indeices: b
	indices := []*model.IndexInfo{
		{
			Name: ast.NewCIStr("b"),
			Columns: []*model.IndexColumn{
				{
					Name:   ast.NewCIStr("b"),
					Length: types.UnspecifiedLength,
					Offset: 1,
				},
			},
			State:  model.StatePublic,
			Unique: true,
		},
	}
	pkColumn := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    0,
		Name:      ast.NewCIStr("a"),
		FieldType: newLongType(),
		ID:        1,
	}
	col0 := &model.ColumnInfo{
		State:     model.StatePublic,
		Offset:    1,
		Name:      ast.NewCIStr("b"),
		FieldType: newLongType(),
		ID:        2,
	}
	col1 := &model.ColumnInfo{
		State:     model.StateNone,
		Offset:    2,
		Name:      ast.NewCIStr("c"),
		FieldType: newLongType(),
		ID:        3,
	}
	pkColumn.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag)
	col0.SetFlag(mysql.NotNullFlag)
	col1.SetFlag(mysql.UnsignedFlag)
	table := &model.TableInfo{
		ID:         8,
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1},
		Indices:    indices,
		Name:       ast.NewCIStr("T_StateNoneColumn"),
		PKIsHandle: true,
		State:      model.StatePublic,
	}
	return table
}
