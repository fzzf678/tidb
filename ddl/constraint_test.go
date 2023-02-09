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

package ddl_test

import (
	"sort"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestCreateTableWithCheckConstraints(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	// Test column-type check constraint.
	tk.MustExec("create table t(a int not null check(a>0))")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	require.Equal(t, 1, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints
	require.Equal(t, int64(1), constrs[0].ID)
	require.True(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)
	require.Equal(t, "`a` > 0", constrs[0].ExprString)

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a bigint key constraint my_constr check(a<10), b int constraint check(b > 1) not enforced)")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, 2, len(constrs))
	require.Equal(t, int64(1), constrs[0].ID)
	require.True(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("my_constr"), constrs[0].Name)
	require.Equal(t, "`a` < 10", constrs[0].ExprString)

	require.Equal(t, int64(2), constrs[1].ID)
	require.True(t, constrs[1].InColumn)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("b"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`b` > 1", constrs[1].ExprString)

	// Test table-type check constraint.
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int constraint check(a > 1) not enforced, constraint my_constr check(a < 10))")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, 2, len(constrs))
	// table-type check constraint.
	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("my_constr"), constrs[0].Name)
	require.Equal(t, "`a` < 10", constrs[0].ExprString)

	// column-type check constraint.
	require.Equal(t, int64(2), constrs[1].ID)
	require.True(t, constrs[1].InColumn)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`a` > 1", constrs[1].ExprString)

	// Test column-type check constraint fail on dependency.
	tk.MustExec("drop table t")
	_, err := tk.Exec("create table t(a int not null check(b>0))")
	require.Errorf(t, err, "[ddl:3813]Column check constraint 't_chk_1' references other column.")

	_, err = tk.Exec("create table t(a int not null check(b>a))")
	require.Errorf(t, err, "[ddl:3813]Column check constraint 't_chk_1' references other column.")

	_, err = tk.Exec("create table t(a int not null check(a>0), b int, constraint check(c>b))")
	require.Errorf(t, err, "[ddl:3820]Check constraint 't_chk_1' refers to non-existing column 'c'.")

	tk.MustExec("create table t(a int not null check(a>0), b int, constraint check(a>b))")
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a int not null check(a > '12345'))")
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a int not null primary key check(a > '12345'))")
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a varchar(10) not null primary key check(a > '12345'))")
	tk.MustExec("drop table t")
}

func TestAlterTableAddCheckConstraints(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int not null check(a>0))")
	// Add constraint with name.
	tk.MustExec("alter table t add constraint haha check(a<10)")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints
	require.Equal(t, int64(2), constrs[1].ID)
	require.False(t, constrs[1].InColumn)
	require.True(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("haha"), constrs[1].Name)
	require.Equal(t, "`a` < 10", constrs[1].ExprString)

	// Add constraint without name.
	tk.MustExec("alter table t add constraint check(a<11) not enforced")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	require.Equal(t, 3, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, int64(3), constrs[2].ID)
	require.False(t, constrs[2].InColumn)
	require.False(t, constrs[2].Enforced)
	require.Equal(t, "t", constrs[2].Table.L)
	require.Equal(t, model.StatePublic, constrs[2].State)
	require.Equal(t, 1, len(constrs[2].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[2].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_2"), constrs[2].Name)
	require.Equal(t, "`a` < 11", constrs[2].ExprString)

	// Add constraint with the name has already existed.
	_, err := tk.Exec("alter table t add constraint haha check(a)")
	require.Errorf(t, err, "[schema:3822]Duplicate check constraint name 'haha'.")

	// Add constraint with the unknown column.
	_, err = tk.Exec("alter table t add constraint check(b)")
	require.Errorf(t, err, "[ddl:3820]Check constraint 't_chk_3' refers to non-existing column 'b'.")

	tk.MustExec("alter table t add constraint check(a*2 < a+1) not enforced")
}

func TestAlterTableDropCheckConstraints(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int not null check(a>0), b int, constraint haha check(a < b), check(a<b+1))")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 3, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints

	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 2, len(constrs[0].ConstraintCols))
	sort.Slice(constrs[0].ConstraintCols, func(i, j int) bool {
		return constrs[0].ConstraintCols[i].L < constrs[0].ConstraintCols[j].L
	})
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("b"), constrs[0].ConstraintCols[1])
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.Equal(t, "`a` < `b`", constrs[0].ExprString)

	require.Equal(t, int64(2), constrs[1].ID)
	require.False(t, constrs[1].InColumn)
	require.True(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 2, len(constrs[1].ConstraintCols))
	sort.Slice(constrs[1].ConstraintCols, func(i, j int) bool {
		return constrs[1].ConstraintCols[i].L < constrs[1].ConstraintCols[j].L
	})
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("b"), constrs[1].ConstraintCols[1])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`a` < `b` + 1", constrs[1].ExprString)

	// Column check constraint will be appended to the table constraint list.
	// So the offset will be the last one, so is the id.
	require.Equal(t, int64(3), constrs[2].ID)
	require.True(t, constrs[2].InColumn)
	require.True(t, constrs[2].Enforced)
	require.Equal(t, "t", constrs[2].Table.L)
	require.Equal(t, model.StatePublic, constrs[2].State)
	require.Equal(t, 1, len(constrs[2].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[2].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_2"), constrs[2].Name)
	require.Equal(t, "`a` > 0", constrs[2].ExprString)

	// Drop a non-exist constraint
	_, err := tk.Exec("alter table t drop constraint not_exist_constraint")
	require.Errorf(t, err, "[ddl:3940]Constraint 'not_exist_constraint' does not exist")

	tk.MustExec("alter table t drop constraint haha")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)
	require.Equal(t, model.NewCIStr("t_chk_2"), constrs[1].Name)

	tk.MustExec("alter table t drop constraint t_chk_2")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 1, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)
}

func TestAlterTableAlterCheckConstraints(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int not null check(a>0) not enforced, b int, constraint haha check(a < b))")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints

	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 2, len(constrs[0].ConstraintCols))
	sort.Slice(constrs[0].ConstraintCols, func(i, j int) bool {
		return constrs[0].ConstraintCols[i].L < constrs[0].ConstraintCols[j].L
	})
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("b"), constrs[0].ConstraintCols[1])
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.Equal(t, "`a` < `b`", constrs[0].ExprString)

	require.Equal(t, int64(2), constrs[1].ID)
	require.True(t, constrs[1].InColumn)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`a` > 0", constrs[1].ExprString)

	// Alter constraint alter constraint with unknown name.
	_, err := tk.Exec("alter table t alter constraint unknown not enforced")
	require.Errorf(t, err, "[ddl:3940]Constraint 'unknown' does not exist")

	// Alter table alter constraint with user name.
	tk.MustExec("alter table t alter constraint haha not enforced")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.False(t, constrs[0].Enforced)
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)

	// Alter table alter constraint with generated name.
	tk.MustExec("alter table t alter constraint t_chk_1 enforced")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.False(t, constrs[0].Enforced)
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.True(t, constrs[1].Enforced)
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)

	// Alter table alter constraint will violate check.
	// Here a=1, b=0 doesn't satisfy "a < b" constraint.
	// Since "a<b" is not enforced, so the insert will success.

	//tk.MustExec("insert into t values(1, 0)")
	//_, err = tk.Exec("alter table t alter constraint haha enforced")
	//require.Errorf(t, err, "[table:3819]Check constraint 'haha' is violated."))
}
