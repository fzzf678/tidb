// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
)

func TestSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("SET @a = 1;")
	tk.MustExec(`SET @a = "1";`)
	tk.MustExec("SET @a = null;")
	tk.MustExec("SET @@global.autocommit = 1;")

	// TODO: this test case should returns error.
	// err := tk.ExecToErr("SET @@global.autocommit = null;")
	// c.Assert(err, NotNil)

	tk.MustExec("SET @@autocommit = 1;")
	require.Error(t, tk.ExecToErr("SET @@autocommit = null;"))
	require.Error(t, tk.ExecToErr("SET @@date_format = 1;"))
	require.Error(t, tk.ExecToErr("SET @@rewriter_enabled = 1;"))
	require.Error(t, tk.ExecToErr("SET xxx = abcd;"))
	require.Error(t, tk.ExecToErr("SET @@global.a = 1;"))
	require.Error(t, tk.ExecToErr("SET @@global.timestamp = 1;"))

	// For issue 998
	tk.MustExec("SET @issue998a=1, @issue998b=5;")
	tk.MustQuery(`select @issue998a, @issue998b;`).Check(testkit.Rows("1 5"))
	tk.MustExec("SET @@autocommit=0, @issue998a=2;")
	tk.MustQuery(`select @issue998a, @@autocommit;`).Check(testkit.Rows("2 0"))
	tk.MustExec("SET @@global.autocommit=1, @issue998b=6;")
	tk.MustQuery(`select @issue998b, @@global.autocommit;`).Check(testkit.Rows("6 1"))

	// For issue 4302
	tk.MustExec("use test;drop table if exists x;create table x(a int);insert into x value(1);")
	tk.MustExec("SET @issue4302=(select a from x limit 1);")
	tk.MustQuery(`select @issue4302;`).Check(testkit.Rows("1"))

	// Set default
	// {ScopeGlobal | ScopeSession, "low_priority_updates", "OFF"},
	// For global var
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.low_priority_updates="ON";`)
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("1"))
	tk.MustExec(`set @@global.low_priority_updates=DEFAULT;`) // It will be set to default var value.
	tk.MustQuery(`select @@global.low_priority_updates;`).Check(testkit.Rows("0"))
	// For session
	tk.MustQuery(`select @@session.low_priority_updates;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.low_priority_updates="ON";`)
	tk.MustExec(`set @@session.low_priority_updates=DEFAULT;`) // It will be set to global var value.
	tk.MustQuery(`select @@session.low_priority_updates;`).Check(testkit.Rows("1"))

	// For mysql jdbc driver issue.
	tk.MustQuery(`select @@session.tx_read_only;`).Check(testkit.Rows("0"))

	// Test session variable states.
	vars := tk.Session().(sessionctx.Context).GetSessionVars()
	require.NoError(t, tk.Session().CommitTxn(context.TODO()))
	tk.MustExec("set @@autocommit = 1")
	require.False(t, vars.InTxn())
	require.True(t, vars.IsAutocommit())
	tk.MustExec("set @@autocommit = 0")
	require.False(t, vars.IsAutocommit())

	tk.MustExec("set @@sql_mode = 'strict_trans_tables'")
	require.True(t, vars.SQLMode.HasStrictMode())
	tk.MustExec("set @@sql_mode = ''")
	require.False(t, vars.SQLMode.HasStrictMode())

	tk.MustExec("set names utf8")
	charset, collation := vars.GetCharsetInfo()
	require.Equal(t, "utf8", charset)
	require.Equal(t, "utf8_bin", collation)

	tk.MustExec("set names latin1 collate latin1_bin")
	charset, collation = vars.GetCharsetInfo()
	require.Equal(t, "latin1", charset)
	require.Equal(t, "latin1_bin", collation)

	tk.MustExec("set names utf8 collate default")
	charset, collation = vars.GetCharsetInfo()
	require.Equal(t, "utf8", charset)
	require.Equal(t, "utf8_bin", collation)

	expectErrMsg := "[ddl:1273]Unknown collation: 'non_exist_collation'"
	tk.MustGetErrMsg("set names utf8 collate non_exist_collation", expectErrMsg)
	tk.MustGetErrMsg("set @@session.collation_server='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@session.collation_database='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@session.collation_connection='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.collation_server='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.collation_database='non_exist_collation'", expectErrMsg)
	tk.MustGetErrMsg("set @@global.collation_connection='non_exist_collation'", expectErrMsg)

	expectErrMsg = "[parser:1115]Unknown character set: 'boguscharsetname'"
	tk.MustGetErrMsg("set names boguscharsetname", expectErrMsg)

	tk.MustExec("set character_set_results = NULL")
	tk.MustQuery("select @@character_set_results").Check(testkit.Rows(""))

	tk.MustExec("set @@global.ddl_slow_threshold=12345")
	tk.MustQuery("select @@global.ddl_slow_threshold").Check(testkit.Rows("12345"))
	require.Equal(t, uint32(12345), vardef.DDLSlowOprThreshold)
	tk.MustExec("set session ddl_slow_threshold=\"54321\"")
	tk.MustQuery("show variables like 'ddl_slow_threshold'").Check(testkit.Rows("ddl_slow_threshold 54321"))
	require.Equal(t, uint32(54321), vardef.DDLSlowOprThreshold)
	tk.MustExec("set @@global.ddl_slow_threshold=-1")
	tk.MustQuery("select @@global.ddl_slow_threshold").Check(testkit.Rows(strconv.Itoa(vardef.DefTiDBDDLSlowOprThreshold)))
	require.Equal(t, uint32(vardef.DefTiDBDDLSlowOprThreshold), vardef.DDLSlowOprThreshold)
	require.Error(t, tk.ExecToErr("set @@global.ddl_slow_threshold=abc"))
	tk.MustQuery("select @@global.ddl_slow_threshold").Check(testkit.Rows(strconv.Itoa(vardef.DefTiDBDDLSlowOprThreshold)))
	require.Equal(t, uint32(vardef.DefTiDBDDLSlowOprThreshold), vardef.DDLSlowOprThreshold)

	// Test set transaction isolation level, which is equivalent to setting variable "tx_isolation".
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	// error
	err := tk.ExecToErr("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	// Fails
	err = tk.ExecToErr("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	// test synonyms variables
	tk.MustExec("SET SESSION tx_isolation = 'READ-COMMITTED'")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	err = tk.ExecToErr("SET SESSION tx_isolation = 'READ-UNCOMMITTED'")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// fails
	err = tk.ExecToErr("SET SESSION transaction_isolation = 'SERIALIZABLE'")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// fails
	err = tk.ExecToErr("SET GLOBAL transaction_isolation = 'SERIALIZABLE'")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	err = tk.ExecToErr("SET GLOBAL transaction_isolation = 'READ-UNCOMMITTED'")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	err = tk.ExecToErr("SET GLOBAL tx_isolation = 'SERIALIZABLE'")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("REPEATABLE-READ"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("REPEATABLE-READ"))

	// Even the transaction fail, set session variable would success.
	tk.MustExec("BEGIN")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	require.Error(t, tk.ExecToErr(`INSERT INTO t VALUES ("sdfsdf")`))
	tk.MustExec("COMMIT")
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("set global avoid_temporal_upgrade = on")
	tk.MustQuery(`select @@global.avoid_temporal_upgrade;`).Check(testkit.Rows("1"))
	tk.MustExec("set @@global.avoid_temporal_upgrade = off")
	tk.MustQuery(`select @@global.avoid_temporal_upgrade;`).Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_general_log = 1")
	tk.MustExec("set @@tidb_general_log = 0")

	tk.MustExec("set @@tidb_pprof_sql_cpu = 1")
	tk.MustExec("set @@tidb_pprof_sql_cpu = 0")

	tk.MustExec(`set @@block_encryption_mode = "aes-128-ecb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-128-ecb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-192-ecb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-192-ecb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-256-ecb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-256-ecb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-128-cbc"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-128-cbc"))
	tk.MustExec(`set @@block_encryption_mode = "aes-192-cbc"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-192-cbc"))
	tk.MustExec(`set @@block_encryption_mode = "aes-256-cbc"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-256-cbc"))
	tk.MustExec(`set @@block_encryption_mode = "aes-128-ofb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-128-ofb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-192-ofb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-192-ofb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-256-ofb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-256-ofb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-128-cfb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-128-cfb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-192-cfb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-192-cfb"))
	tk.MustExec(`set @@block_encryption_mode = "aes-256-cfb"`)
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-256-cfb"))
	require.Error(t, tk.ExecToErr("set @@block_encryption_mode = 'abc'"))
	tk.MustQuery(`select @@block_encryption_mode;`).Check(testkit.Rows("aes-256-cfb"))

	tk.MustExec(`set @@global.tidb_force_priority = "no_priority"`)
	tk.MustQuery(`select @@global.tidb_force_priority;`).Check(testkit.Rows("NO_PRIORITY"))
	tk.MustExec(`set @@global.tidb_force_priority = "low_priority"`)
	tk.MustQuery(`select @@global.tidb_force_priority;`).Check(testkit.Rows("LOW_PRIORITY"))
	tk.MustExec(`set @@global.tidb_force_priority = "high_priority"`)
	tk.MustQuery(`select @@global.tidb_force_priority;`).Check(testkit.Rows("HIGH_PRIORITY"))
	tk.MustExec(`set @@global.tidb_force_priority = "delayed"`)
	tk.MustQuery(`select @@global.tidb_force_priority;`).Check(testkit.Rows("DELAYED"))
	require.Error(t, tk.ExecToErr("set global tidb_force_priority = 'abc'"))
	tk.MustQuery(`select @@global.tidb_force_priority;`).Check(testkit.Rows("DELAYED"))

	tk.MustExec(`set @@session.tidb_ddl_reorg_priority = "priority_low"`)
	tk.MustQuery(`select @@session.tidb_ddl_reorg_priority;`).Check(testkit.Rows("PRIORITY_LOW"))
	tk.MustExec(`set @@session.tidb_ddl_reorg_priority = "priority_normal"`)
	tk.MustQuery(`select @@session.tidb_ddl_reorg_priority;`).Check(testkit.Rows("PRIORITY_NORMAL"))
	tk.MustExec(`set @@session.tidb_ddl_reorg_priority = "priority_high"`)
	tk.MustQuery(`select @@session.tidb_ddl_reorg_priority;`).Check(testkit.Rows("PRIORITY_HIGH"))
	require.Error(t, tk.ExecToErr("set session tidb_ddl_reorg_priority = 'abc'"))
	tk.MustQuery(`select @@session.tidb_ddl_reorg_priority;`).Check(testkit.Rows("PRIORITY_HIGH"))

	tk.MustExec("set tidb_opt_write_row_id = 1")
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_opt_write_row_id = 0")
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("0"))
	tk.MustExec("set tidb_opt_write_row_id = true")
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_opt_write_row_id = false")
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("0"))
	tk.MustExec("set tidb_opt_write_row_id = On")
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_opt_write_row_id = Off")
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("set tidb_opt_write_row_id = 'abc'"))
	tk.MustQuery(`select @@session.tidb_opt_write_row_id;`).Check(testkit.Rows("0"))

	tk.MustExec("set tidb_checksum_table_concurrency = 42")
	tk.MustQuery(`select @@tidb_checksum_table_concurrency;`).Check(testkit.Rows("42"))
	require.Error(t, tk.ExecToErr("set tidb_checksum_table_concurrency = 'abc'"))
	tk.MustQuery(`select @@tidb_checksum_table_concurrency;`).Check(testkit.Rows("42"))
	tk.MustExec("set tidb_checksum_table_concurrency = 257")
	tk.MustQuery(`select @@tidb_checksum_table_concurrency;`).Check(testkit.Rows(strconv.Itoa(vardef.MaxConfigurableConcurrency)))

	tk.MustExec("set tidb_build_stats_concurrency = 42")
	tk.MustQuery(`select @@tidb_build_stats_concurrency;`).Check(testkit.Rows("42"))
	tk.MustExec("set tidb_build_sampling_stats_concurrency = 42")
	tk.MustQuery(`select @@tidb_build_sampling_stats_concurrency;`).Check(testkit.Rows("42"))
	require.Error(t, tk.ExecToErr("set tidb_build_sampling_stats_concurrency = 'abc'"))
	require.Error(t, tk.ExecToErr("set tidb_build_stats_concurrency = 'abc'"))
	tk.MustQuery(`select @@tidb_build_stats_concurrency;`).Check(testkit.Rows("42"))
	tk.MustExec("set tidb_build_stats_concurrency = 257")
	tk.MustQuery(`select @@tidb_build_stats_concurrency;`).Check(testkit.Rows(strconv.Itoa(vardef.MaxConfigurableConcurrency)))
	tk.MustExec("set tidb_build_sampling_stats_concurrency = 257")
	tk.MustQuery(`select @@tidb_build_sampling_stats_concurrency;`).Check(testkit.Rows(strconv.Itoa(vardef.MaxConfigurableConcurrency)))

	tk.MustExec(`set tidb_partition_prune_mode = "static"`)
	tk.MustQuery(`select @@tidb_partition_prune_mode;`).Check(testkit.Rows("static"))
	tk.MustExec(`set tidb_partition_prune_mode = "dynamic"`)
	tk.MustQuery(`select @@tidb_partition_prune_mode;`).Check(testkit.Rows("dynamic"))
	tk.MustExec(`set tidb_partition_prune_mode = "static-only"`)
	tk.MustQuery(`select @@tidb_partition_prune_mode;`).Check(testkit.Rows("static"))
	tk.MustExec(`set tidb_partition_prune_mode = "dynamic-only"`)
	tk.MustQuery(`select @@tidb_partition_prune_mode;`).Check(testkit.Rows("dynamic"))
	require.Error(t, tk.ExecToErr("set tidb_partition_prune_mode = 'abc'"))
	tk.MustQuery(`select @@tidb_partition_prune_mode;`).Check(testkit.Rows("dynamic"))

	tk.MustExec("set tidb_constraint_check_in_place = 1")
	tk.MustQuery(`select @@session.tidb_constraint_check_in_place;`).Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_constraint_check_in_place = 0")
	tk.MustQuery(`select @@global.tidb_constraint_check_in_place;`).Check(testkit.Rows("0"))

	tk.MustExec("set tidb_batch_commit = 0")
	tk.MustQuery("select @@session.tidb_batch_commit;").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_batch_commit = 1")
	tk.MustQuery("select @@session.tidb_batch_commit;").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("set global tidb_batch_commit = 0"))
	require.Error(t, tk.ExecToErr("set global tidb_batch_commit = 2"))

	// test skip isolation level check: init
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// test skip isolation level check: error
	err = tk.ExecToErr("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	err = tk.ExecToErr("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-COMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-COMMITTED"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 8048 The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@session.tx_isolation").Check(testkit.Rows("SERIALIZABLE"))
	tk.MustQuery("select @@session.transaction_isolation").Check(testkit.Rows("SERIALIZABLE"))

	// test skip isolation level check: success
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 1")
	tk.MustExec("SET GLOBAL TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 8048 The isolation level 'READ-UNCOMMITTED' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error"))
	tk.MustQuery("select @@global.tx_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))
	tk.MustQuery("select @@global.transaction_isolation").Check(testkit.Rows("READ-UNCOMMITTED"))

	// test skip isolation level check: reset
	tk.MustExec("SET GLOBAL transaction_isolation='REPEATABLE-READ'") // should reset tx_isolation back to rr before reset tidb_skip_isolation_level_check
	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 0")

	// test for tidb_wait_split_region_finish
	tk.MustQuery(`select @@session.tidb_wait_split_region_finish;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_wait_split_region_finish = 1")
	tk.MustQuery(`select @@session.tidb_wait_split_region_finish;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_wait_split_region_finish = 0")
	tk.MustQuery(`select @@session.tidb_wait_split_region_finish;`).Check(testkit.Rows("0"))

	// test for tidb_scatter_region
	tk.MustQuery(`select @@global.tidb_scatter_region;`).Check(testkit.Rows(""))
	tk.MustExec("set global tidb_scatter_region = 'table'")
	tk.MustQuery(`select @@global.tidb_scatter_region;`).Check(testkit.Rows("table"))
	tk.MustExec("set global tidb_scatter_region = 'global'")
	tk.MustQuery(`select @@global.tidb_scatter_region;`).Check(testkit.Rows("global"))
	tk.MustExec("set session tidb_scatter_region = ''")
	tk.MustQuery(`select @@session.tidb_scatter_region;`).Check(testkit.Rows(""))
	tk.MustExec("set session tidb_scatter_region = 'table'")
	tk.MustQuery(`select @@session.tidb_scatter_region;`).Check(testkit.Rows("table"))
	tk.MustExec("set session tidb_scatter_region = 'global'")
	tk.MustQuery(`select @@session.tidb_scatter_region;`).Check(testkit.Rows("global"))
	require.Error(t, tk.ExecToErr("set session tidb_scatter_region = 'test'"))

	// test for tidb_wait_split_region_timeout
	tk.MustQuery(`select @@session.tidb_wait_split_region_timeout;`).Check(testkit.Rows(strconv.Itoa(vardef.DefWaitSplitRegionTimeout)))
	tk.MustExec("set tidb_wait_split_region_timeout = 1")
	tk.MustQuery(`select @@session.tidb_wait_split_region_timeout;`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_wait_split_region_timeout = 0")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_wait_split_region_timeout value: '0'"))
	tk.MustQuery(`select @@tidb_wait_split_region_timeout`).Check(testkit.Rows("1"))

	tk.MustQuery(`select @@session.tidb_wait_split_region_timeout;`).Check(testkit.Rows("1"))

	tk.MustExec("set session tidb_backoff_weight = 3")
	tk.MustQuery("select @@session.tidb_backoff_weight;").Check(testkit.Rows("3"))
	tk.MustExec("set session tidb_backoff_weight = 20")
	tk.MustQuery("select @@session.tidb_backoff_weight;").Check(testkit.Rows("20"))
	tk.MustExec("set session tidb_backoff_weight = -1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_backoff_weight value: '-1'"))
	tk.MustExec("set global tidb_backoff_weight = 0")
	tk.MustQuery("select @@global.tidb_backoff_weight;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_backoff_weight = 10")
	tk.MustQuery("select @@global.tidb_backoff_weight;").Check(testkit.Rows("10"))

	tk.MustExec("set @@tidb_expensive_query_time_threshold=70")
	tk.MustQuery("select @@tidb_expensive_query_time_threshold;").Check(testkit.Rows("70"))

	tk.MustExec("set @@tidb_expensive_txn_time_threshold=120")
	tk.MustQuery("select @@tidb_expensive_txn_time_threshold;").Check(testkit.Rows("120"))

	tk.MustQuery("select @@global.tidb_store_limit;").Check(testkit.Rows("0"))
	tk.MustExec("set @@global.tidb_store_limit = 100")
	tk.MustQuery("select @@global.tidb_store_limit;").Check(testkit.Rows("100"))
	tk.MustExec("set @@global.tidb_store_limit = 0")
	tk.MustExec("set global tidb_store_limit = 10000")
	tk.MustQuery("select @@global.tidb_store_limit;").Check(testkit.Rows("10000"))

	tk.MustQuery("select @@global.tidb_txn_commit_batch_size;").Check(testkit.Rows("16384"))
	tk.MustExec("set @@global.tidb_txn_commit_batch_size = 100")
	tk.MustQuery("select @@global.tidb_txn_commit_batch_size;").Check(testkit.Rows("100"))
	tk.MustExec("set @@global.tidb_txn_commit_batch_size = 0")
	tk.MustQuery("select @@global.tidb_txn_commit_batch_size;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_txn_commit_batch_size = 100")
	tk.MustQuery("select @@global.tidb_txn_commit_batch_size;").Check(testkit.Rows("100"))

	tk.MustQuery("select @@session.tidb_metric_query_step;").Check(testkit.Rows("60"))
	tk.MustExec("set @@session.tidb_metric_query_step = 120")
	tk.MustExec("set @@session.tidb_metric_query_step = 9")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_metric_query_step value: '9'"))
	tk.MustQuery("select @@session.tidb_metric_query_step;").Check(testkit.Rows("10"))

	tk.MustQuery("select @@session.tidb_metric_query_range_duration;").Check(testkit.Rows("60"))
	tk.MustExec("set @@session.tidb_metric_query_range_duration = 120")
	tk.MustExec("set @@session.tidb_metric_query_range_duration = 9")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_metric_query_range_duration value: '9'"))
	tk.MustQuery("select @@session.tidb_metric_query_range_duration;").Check(testkit.Rows("10"))

	tk.MustExec("set @@cte_max_recursion_depth=100")
	tk.MustQuery("select @@cte_max_recursion_depth").Check(testkit.Rows("100"))
	tk.MustExec("set @@global.cte_max_recursion_depth=100")
	tk.MustQuery("select @@global.cte_max_recursion_depth").Check(testkit.Rows("100"))
	tk.MustExec("set @@cte_max_recursion_depth=-1")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect cte_max_recursion_depth value: '-1'"))
	tk.MustQuery("select @@cte_max_recursion_depth").Check(testkit.Rows("0"))

	// test for tidb_redact_log
	tk.MustQuery(`select @@global.tidb_redact_log;`).Check(testkit.Rows("OFF"))
	tk.MustExec("set global tidb_redact_log = 1")
	tk.MustQuery(`select @@global.tidb_redact_log;`).Check(testkit.Rows("ON"))
	tk.MustExec("set global tidb_redact_log = 0")
	tk.MustQuery(`select @@global.tidb_redact_log;`).Check(testkit.Rows("OFF"))
	tk.MustExec("set session tidb_redact_log = 0")
	tk.MustQuery(`select @@session.tidb_redact_log;`).Check(testkit.Rows("OFF"))
	tk.MustExec("set session tidb_redact_log = 1")
	tk.MustQuery(`select @@session.tidb_redact_log;`).Check(testkit.Rows("ON"))
	tk.MustExec("set session tidb_redact_log = oFf")
	tk.MustQuery(`select @@session.tidb_redact_log;`).Check(testkit.Rows("OFF"))
	tk.MustExec("set session tidb_redact_log = marker")
	tk.MustQuery(`select @@session.tidb_redact_log;`).Check(testkit.Rows("MARKER"))
	tk.MustExec("set session tidb_redact_log = On")
	tk.MustQuery(`select @@session.tidb_redact_log;`).Check(testkit.Rows("ON"))

	tk.MustQuery("select @@tidb_dml_batch_size;").Check(testkit.Rows("0"))
	tk.MustExec("set @@session.tidb_dml_batch_size = 120")
	tk.MustQuery("select @@tidb_dml_batch_size;").Check(testkit.Rows("120"))
	tk.MustExec("set @@session.tidb_dml_batch_size = -120")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_dml_batch_size value: '?'")) // redacted because of tidb_redact_log = 1 above
	tk.MustQuery("select @@session.tidb_dml_batch_size").Check(testkit.Rows("0"))
	tk.MustExec("set session tidb_redact_log = 0")
	tk.MustExec("set session tidb_dml_batch_size = -120")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_dml_batch_size value: '-120'")) // without redaction

	tk.MustExec("set global tidb_gogc_tuner_min_value=300")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set global tidb_gogc_tuner_max_value=600")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set global tidb_gogc_tuner_max_value=600000000000000000")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_gogc_tuner_max_value value: '600000000000000000'"))
	tk.MustExec("set @@session.tidb_dml_batch_size = 120")
	tk.MustExec("set @@global.tidb_dml_batch_size = 200")                    // now permitted due to TiDB #19809
	tk.MustQuery("select @@tidb_dml_batch_size;").Check(testkit.Rows("120")) // global only applies to new sessions

	err = tk.ExecToErr("set tidb_enable_parallel_apply=-1")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar))

	// test for tidb_mem_quota_apply_cache
	defVal := fmt.Sprintf("%v", vardef.DefTiDBMemQuotaApplyCache)
	tk.MustQuery(`select @@tidb_mem_quota_apply_cache`).Check(testkit.Rows(defVal))
	tk.MustExec(`set global tidb_mem_quota_apply_cache = 1`)
	tk.MustQuery(`select @@global.tidb_mem_quota_apply_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`set global tidb_mem_quota_apply_cache = 0`)
	tk.MustQuery(`select @@global.tidb_mem_quota_apply_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`set tidb_mem_quota_apply_cache = 123`)
	tk.MustQuery(`select @@global.tidb_mem_quota_apply_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`select @@tidb_mem_quota_apply_cache`).Check(testkit.Rows("123"))

	// test for tidb_mem_quota_bind_cache
	defVal = fmt.Sprintf("%v", vardef.DefTiDBMemQuotaBindingCache)
	tk.MustQuery(`select @@tidb_mem_quota_binding_cache`).Check(testkit.Rows(defVal))
	tk.MustExec(`set global tidb_mem_quota_binding_cache = 1`)
	tk.MustQuery(`select @@global.tidb_mem_quota_binding_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`set global tidb_mem_quota_binding_cache = 0`)
	tk.MustQuery(`select @@global.tidb_mem_quota_binding_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`set global tidb_mem_quota_binding_cache = 123`)
	tk.MustQuery(`select @@global.tidb_mem_quota_binding_cache`).Check(testkit.Rows("123"))
	tk.MustQuery(`select @@global.tidb_mem_quota_binding_cache`).Check(testkit.Rows("123"))

	// test for tidb_enable_parallel_apply
	tk.MustQuery(`select @@tidb_enable_parallel_apply`).Check(testkit.Rows("0"))
	tk.MustExec(`set global tidb_enable_parallel_apply = 1`)
	tk.MustQuery(`select @@global.tidb_enable_parallel_apply`).Check(testkit.Rows("1"))
	tk.MustExec(`set global tidb_enable_parallel_apply = 0`)
	tk.MustQuery(`select @@global.tidb_enable_parallel_apply`).Check(testkit.Rows("0"))
	tk.MustExec(`set tidb_enable_parallel_apply=1`)
	tk.MustQuery(`select @@global.tidb_enable_parallel_apply`).Check(testkit.Rows("0"))
	tk.MustQuery(`select @@tidb_enable_parallel_apply`).Check(testkit.Rows("1"))

	tk.MustQuery(`select @@global.tidb_general_log;`).Check(testkit.Rows("0"))
	tk.MustQuery(`show variables like 'tidb_general_log';`).Check(testkit.Rows("tidb_general_log OFF"))
	tk.MustExec("set tidb_general_log = 1")
	tk.MustQuery(`select @@global.tidb_general_log;`).Check(testkit.Rows("1"))
	tk.MustQuery(`show variables like 'tidb_general_log';`).Check(testkit.Rows("tidb_general_log ON"))
	tk.MustExec("set tidb_general_log = 0")
	tk.MustQuery(`select @@global.tidb_general_log;`).Check(testkit.Rows("0"))
	tk.MustQuery(`show variables like 'tidb_general_log';`).Check(testkit.Rows("tidb_general_log OFF"))
	tk.MustExec("set tidb_general_log = on")
	tk.MustQuery(`select @@global.tidb_general_log;`).Check(testkit.Rows("1"))
	tk.MustQuery(`show variables like 'tidb_general_log';`).Check(testkit.Rows("tidb_general_log ON"))
	tk.MustExec("set tidb_general_log = off")
	tk.MustQuery(`select @@global.tidb_general_log;`).Check(testkit.Rows("0"))
	tk.MustQuery(`show variables like 'tidb_general_log';`).Check(testkit.Rows("tidb_general_log OFF"))
	require.Error(t, tk.ExecToErr("set tidb_general_log = abc"))
	require.Error(t, tk.ExecToErr("set tidb_general_log = 123"))

	tk.MustExec(`SET @@character_set_results = NULL;`)
	tk.MustQuery(`select @@character_set_results;`).Check(testkit.Rows(""))

	varList := []string{"character_set_server", "character_set_client", "character_set_filesystem", "character_set_database"}
	for _, v := range varList {
		tk.MustGetErrCode(fmt.Sprintf("SET @@global.%s = @global_start_value;", v), mysql.ErrWrongValueForVar)
		tk.MustGetErrCode(fmt.Sprintf("SET @@%s = @global_start_value;", v), mysql.ErrWrongValueForVar)
		tk.MustGetErrCode(fmt.Sprintf("SET @@%s = NULL;", v), mysql.ErrWrongValueForVar)
		tk.MustGetErrCode(fmt.Sprintf("SET @@%s = \"\";", v), mysql.ErrWrongValueForVar)
		tk.MustGetErrMsg(fmt.Sprintf("SET @@%s = \"somecharset\";", v), "Unknown charset somecharset")
		// we do not support set character_set_xxx or collation_xxx to a collation id.
		tk.MustGetErrMsg(fmt.Sprintf("SET @@global.%s = 46;", v), "Unknown charset 46")
		tk.MustGetErrMsg(fmt.Sprintf("SET @@%s = 46;", v), "Unknown charset 46")
	}

	tk.MustExec("SET SESSION tidb_enable_extended_stats = on")
	tk.MustQuery("select @@session.tidb_enable_extended_stats").Check(testkit.Rows("1"))
	tk.MustExec("SET SESSION tidb_enable_extended_stats = off")
	tk.MustQuery("select @@session.tidb_enable_extended_stats").Check(testkit.Rows("0"))
	tk.MustExec("SET GLOBAL tidb_enable_extended_stats = on")
	tk.MustQuery("select @@global.tidb_enable_extended_stats").Check(testkit.Rows("1"))
	tk.MustExec("SET GLOBAL tidb_enable_extended_stats = off")
	tk.MustQuery("select @@global.tidb_enable_extended_stats").Check(testkit.Rows("0"))

	tk.MustExec("SET SESSION tidb_allow_fallback_to_tikv = 'tiflash'")
	tk.MustQuery("select @@session.tidb_allow_fallback_to_tikv").Check(testkit.Rows("tiflash"))
	tk.MustExec("SET SESSION tidb_allow_fallback_to_tikv = ''")
	tk.MustQuery("select @@session.tidb_allow_fallback_to_tikv").Check(testkit.Rows(""))
	tk.MustExec("SET GLOBAL tidb_allow_fallback_to_tikv = 'tiflash'")
	tk.MustQuery("select @@global.tidb_allow_fallback_to_tikv").Check(testkit.Rows("tiflash"))
	tk.MustExec("SET GLOBAL tidb_allow_fallback_to_tikv = ''")
	tk.MustQuery("select @@global.tidb_allow_fallback_to_tikv").Check(testkit.Rows(""))
	tk.MustExec("set @@tidb_allow_fallback_to_tikv = 'tiflash, tiflash, tiflash'")
	tk.MustQuery("select @@tidb_allow_fallback_to_tikv").Check(testkit.Rows("tiflash"))

	tk.MustGetErrMsg("SET SESSION tidb_allow_fallback_to_tikv = 'tikv,tiflash'", "[variable:1231]Variable 'tidb_allow_fallback_to_tikv' can't be set to the value of 'tikv,tiflash'")
	tk.MustGetErrMsg("SET GLOBAL tidb_allow_fallback_to_tikv = 'tikv,tiflash'", "[variable:1231]Variable 'tidb_allow_fallback_to_tikv' can't be set to the value of 'tikv,tiflash'")
	tk.MustGetErrMsg("set @@tidb_allow_fallback_to_tikv = 'tidb, tiflash, tiflash'", "[variable:1231]Variable 'tidb_allow_fallback_to_tikv' can't be set to the value of 'tidb, tiflash, tiflash'")
	tk.MustGetErrMsg("set @@tidb_allow_fallback_to_tikv = 'unknown, tiflash, tiflash'", "[variable:1231]Variable 'tidb_allow_fallback_to_tikv' can't be set to the value of 'unknown, tiflash, tiflash'")

	// Test issue #22145
	tk.MustExec(`set global sync_relay_log = "'"`)

	tk.MustExec(`set @@global.tidb_enable_clustered_index = 'int_only'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1287 'INT_ONLY' is deprecated and will be removed in a future release. Please use 'ON' or 'OFF' instead"))
	tk.MustExec(`set @@global.tidb_enable_clustered_index = 'off'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec("set @@tidb_enable_clustered_index = 'off'")
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec("set @@tidb_enable_clustered_index = 'on'")
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec("set @@tidb_enable_clustered_index = 'int_only'")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1287 'INT_ONLY' is deprecated and will be removed in a future release. Please use 'ON' or 'OFF' instead"))

	// test for tidb_enable_ordered_result_mode
	tk.MustQuery(`select @@tidb_enable_ordered_result_mode`).Check(testkit.Rows("0"))
	tk.MustExec(`set global tidb_enable_ordered_result_mode = 1`)
	tk.MustQuery(`select @@global.tidb_enable_ordered_result_mode`).Check(testkit.Rows("1"))
	tk.MustExec(`set global tidb_enable_ordered_result_mode = 0`)
	tk.MustQuery(`select @@global.tidb_enable_ordered_result_mode`).Check(testkit.Rows("0"))
	tk.MustExec(`set tidb_enable_ordered_result_mode=1`)
	tk.MustQuery(`select @@global.tidb_enable_ordered_result_mode`).Check(testkit.Rows("0"))
	tk.MustQuery(`select @@tidb_enable_ordered_result_mode`).Check(testkit.Rows("1"))

	// test for tidb_opt_enable_correlation_adjustment
	tk.MustQuery(`select @@tidb_opt_enable_correlation_adjustment`).Check(testkit.Rows("1"))
	tk.MustExec(`set global tidb_opt_enable_correlation_adjustment = 0`)
	tk.MustQuery(`select @@global.tidb_opt_enable_correlation_adjustment`).Check(testkit.Rows("0"))
	tk.MustExec(`set global tidb_opt_enable_correlation_adjustment = 1`)
	tk.MustQuery(`select @@global.tidb_opt_enable_correlation_adjustment`).Check(testkit.Rows("1"))
	tk.MustExec(`set tidb_opt_enable_correlation_adjustment=0`)
	tk.MustQuery(`select @@global.tidb_opt_enable_correlation_adjustment`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@tidb_opt_enable_correlation_adjustment`).Check(testkit.Rows("0"))

	// test for tidb_opt_limit_push_down_threshold
	tk.MustQuery(`select @@tidb_opt_limit_push_down_threshold`).Check(testkit.Rows("100"))
	tk.MustExec(`set global tidb_opt_limit_push_down_threshold = 20`)
	tk.MustQuery(`select @@global.tidb_opt_limit_push_down_threshold`).Check(testkit.Rows("20"))
	tk.MustExec(`set global tidb_opt_limit_push_down_threshold = 100`)
	tk.MustQuery(`select @@global.tidb_opt_limit_push_down_threshold`).Check(testkit.Rows("100"))
	tk.MustExec(`set tidb_opt_limit_push_down_threshold = 20`)
	tk.MustQuery(`select @@global.tidb_opt_limit_push_down_threshold`).Check(testkit.Rows("100"))
	tk.MustQuery(`select @@tidb_opt_limit_push_down_threshold`).Check(testkit.Rows("20"))

	tk.MustQuery("select @@tidb_opt_prefer_range_scan").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_opt_prefer_range_scan = 1")
	tk.MustQuery("select @@global.tidb_opt_prefer_range_scan").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_opt_prefer_range_scan = 0")
	tk.MustQuery("select @@global.tidb_opt_prefer_range_scan").Check(testkit.Rows("0"))
	tk.MustExec("set session tidb_opt_prefer_range_scan = 1")
	tk.MustQuery("select @@session.tidb_opt_prefer_range_scan").Check(testkit.Rows("1"))
	tk.MustExec("set session tidb_opt_prefer_range_scan = 0")
	tk.MustQuery("select @@session.tidb_opt_prefer_range_scan").Check(testkit.Rows("0"))

	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 0.5")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("0.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1.5")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("1.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("10"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -0.01")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-0.01'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10.01")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_tso_client_batch_max_wait_time value: '10.01'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("10"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10.1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_tso_client_batch_max_wait_time value: '10.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time").Check(testkit.Rows("10"))
	require.Error(t, tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 1"))

	tk.MustQuery("select @@tidb_enable_tso_follower_proxy").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = 1")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = 0")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy").Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("set tidb_enable_tso_follower_proxy = 1"))
	tk.MustQuery("select @@pd_enable_follower_handle_region").Check(testkit.Rows("1"))
	tk.MustExec("set global pd_enable_follower_handle_region = 0")
	tk.MustQuery("select @@pd_enable_follower_handle_region").Check(testkit.Rows("0"))
	tk.MustExec("set global pd_enable_follower_handle_region = 1")
	tk.MustQuery("select @@pd_enable_follower_handle_region").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("set pd_enable_follower_handle_region = 1"))
	tk.MustQuery("select @@tidb_enable_batch_query_region").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_batch_query_region = 1")
	tk.MustQuery("select @@tidb_enable_batch_query_region").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("set tidb_enable_batch_query_region = 1"))

	tk.MustQuery("select @@tidb_enable_historical_stats").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustQuery("select @@tidb_enable_historical_stats").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustQuery("select @@tidb_enable_historical_stats").Check(testkit.Rows("0"))

	// test for tidb_enable_column_tracking
	tk.MustQuery("select @@tidb_enable_column_tracking").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_column_tracking = 0")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1681 The 'tidb_enable_column_tracking' variable is deprecated and will be removed in future versions of TiDB. It is always set to 'ON' now."))
	tk.MustQuery("select @@tidb_enable_column_tracking").Check(testkit.Rows("1"))
	tk.MustQuery("select count(1) from mysql.tidb where variable_name = 'tidb_disable_column_tracking_time' and variable_value is not null").Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("select @@session.tidb_enable_column_tracking"))
	require.Error(t, tk.ExecToErr("set tidb_enable_column_tracking = 0"))
	require.Error(t, tk.ExecToErr("set global tidb_enable_column_tracking = -1"))

	// test for tidb_analyze_column_options
	tk.MustQuery("select @@tidb_analyze_column_options").Check(testkit.Rows("PREDICATE"))
	tk.MustExec("set global tidb_analyze_column_options = 'ALL'")
	tk.MustQuery("select @@tidb_analyze_column_options").Check(testkit.Rows("ALL"))
	tk.MustExec("set global tidb_analyze_column_options = 'predicate'")
	tk.MustQuery("select @@tidb_analyze_column_options").Check(testkit.Rows("PREDICATE"))
	require.Error(t, tk.ExecToErr("set global tidb_analyze_column_options = 'UNKNOWN'"))

	// test for tidb_ignore_prepared_cache_close_stmt
	tk.MustQuery("select @@global.tidb_ignore_prepared_cache_close_stmt").Check(testkit.Rows("0")) // default value is 0
	tk.MustExec("set global tidb_ignore_prepared_cache_close_stmt=1")
	tk.MustQuery("select @@global.tidb_ignore_prepared_cache_close_stmt").Check(testkit.Rows("1"))
	tk.MustQuery("show global variables like 'tidb_ignore_prepared_cache_close_stmt'").Check(testkit.Rows("tidb_ignore_prepared_cache_close_stmt ON"))
	tk.MustExec("set global tidb_ignore_prepared_cache_close_stmt=0")
	tk.MustQuery("select @@global.tidb_ignore_prepared_cache_close_stmt").Check(testkit.Rows("0"))
	tk.MustQuery("show global variables like 'tidb_ignore_prepared_cache_close_stmt'").Check(testkit.Rows("tidb_ignore_prepared_cache_close_stmt OFF"))

	// test for tidb_enable_new_cost_interface
	tk.MustQuery("select @@global.tidb_enable_new_cost_interface").Check(testkit.Rows("1")) // default value is 1
	tk.MustExec("set global tidb_enable_new_cost_interface=0")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1287 'OFF' is deprecated and will be removed in a future release. Please use ON instead"))
	tk.MustQuery("select @@global.tidb_enable_new_cost_interface").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_new_cost_interface=0")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1287 'OFF' is deprecated and will be removed in a future release. Please use ON instead"))
	tk.MustQuery("select @@session.tidb_enable_new_cost_interface").Check(testkit.Rows("1"))

	// test for tidb_remove_orderby_in_subquery
	tk.MustQuery("select @@session.tidb_remove_orderby_in_subquery").Check(testkit.Rows("1")) // default value is 1
	tk.MustExec("set session tidb_remove_orderby_in_subquery=0")
	tk.MustQuery("select @@session.tidb_remove_orderby_in_subquery").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.tidb_remove_orderby_in_subquery").Check(testkit.Rows("1")) // default value is 1
	tk.MustExec("set global tidb_remove_orderby_in_subquery=0")
	tk.MustQuery("select @@global.tidb_remove_orderby_in_subquery").Check(testkit.Rows("0"))

	// test for tidb_opt_skew_distinct_agg
	tk.MustQuery("select @@session.tidb_opt_skew_distinct_agg").Check(testkit.Rows("0")) // default value is 0
	tk.MustExec("set session tidb_opt_skew_distinct_agg=1")
	tk.MustQuery("select @@session.tidb_opt_skew_distinct_agg").Check(testkit.Rows("1"))
	tk.MustQuery("select @@global.tidb_opt_skew_distinct_agg").Check(testkit.Rows("0")) // default value is 0
	tk.MustExec("set global tidb_opt_skew_distinct_agg=1")
	tk.MustQuery("select @@global.tidb_opt_skew_distinct_agg").Check(testkit.Rows("1"))

	// test for tidb_opt_three_stage_distinct_agg
	tk.MustQuery("select @@session.tidb_opt_three_stage_distinct_agg").Check(testkit.Rows("1")) // default value is 1
	tk.MustExec("set session tidb_opt_three_stage_distinct_agg=0")
	tk.MustQuery("select @@session.tidb_opt_three_stage_distinct_agg").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.tidb_opt_three_stage_distinct_agg").Check(testkit.Rows("1")) // default value is 1
	tk.MustExec("set global tidb_opt_three_stage_distinct_agg=0")
	tk.MustQuery("select @@global.tidb_opt_three_stage_distinct_agg").Check(testkit.Rows("0"))

	// the value of max_allowed_packet should be a multiple of 1024
	tk.MustExec("set @@global.max_allowed_packet=16385")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_allowed_packet value: '16385'"))
	result := tk.MustQuery("select @@global.max_allowed_packet;")
	result.Check(testkit.Rows("16384"))
	tk.MustExec("set @@global.max_allowed_packet=2047")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_allowed_packet value: '2047'"))
	result = tk.MustQuery("select @@global.max_allowed_packet;")
	result.Check(testkit.Rows("1024"))
	tk.MustExec("set @@global.max_allowed_packet=0")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_allowed_packet value: '0'"))
	result = tk.MustQuery("select @@global.max_allowed_packet;")
	result.Check(testkit.Rows("1024"))

	// test value of tidb_stats_cache_mem_quota
	tk.MustQuery("select @@global.tidb_stats_cache_mem_quota").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_stats_cache_mem_quota = 200")
	tk.MustQuery("select @@global.tidb_stats_cache_mem_quota").Check(testkit.Rows("200"))
	// assert quota must larger than -1
	tk.MustExec("set global tidb_stats_cache_mem_quota = -1")
	tk.MustQuery("select @@global.tidb_stats_cache_mem_quota").Check(testkit.Rows("0"))
	// assert quota muster smaller than 1TB
	tk.MustExec("set global tidb_stats_cache_mem_quota = 1099511627777")
	tk.MustQuery("select @@global.tidb_stats_cache_mem_quota").Check(testkit.Rows("1099511627776"))
	// for read-only instance scoped system variables.
	tk.MustGetErrCode("set @@global.plugin_load = ''", errno.ErrIncorrectGlobalLocalVar)
	tk.MustGetErrCode("set @@global.plugin_dir = ''", errno.ErrIncorrectGlobalLocalVar)

	// test for tidb_max_auto_analyze_time
	tk.MustQuery("select @@tidb_max_auto_analyze_time").Check(testkit.Rows(strconv.Itoa(vardef.DefTiDBMaxAutoAnalyzeTime)))
	tk.MustExec("set global tidb_max_auto_analyze_time = 60")
	tk.MustQuery("select @@tidb_max_auto_analyze_time").Check(testkit.Rows("60"))
	tk.MustExec("set global tidb_max_auto_analyze_time = -1")
	tk.MustQuery("select @@tidb_max_auto_analyze_time").Check(testkit.Rows("0"))

	// test for instance plan cache variables
	tk.MustQuery("select @@global.tidb_enable_instance_plan_cache").Check(testkit.Rows("0")) // default 0
	tk.MustQuery("select @@global.tidb_instance_plan_cache_max_size").Check(testkit.Rows("104857600"))
	tk.MustExec("set global tidb_instance_plan_cache_max_size = 135829120")
	tk.MustQuery("select @@global.tidb_instance_plan_cache_max_size").Check(testkit.Rows("135829120"))
	tk.MustExec("set global tidb_instance_plan_cache_max_size = 999999999")
	tk.MustQuery("select @@global.tidb_instance_plan_cache_max_size").Check(testkit.Rows("999999999"))
	tk.MustExec("set global tidb_instance_plan_cache_max_size = 1GiB")
	tk.MustQuery("select @@global.tidb_instance_plan_cache_max_size").Check(testkit.Rows("1073741824"))
	tk.MustExec("set global tidb_instance_plan_cache_max_size = 2GiB")
	tk.MustQuery("select @@global.tidb_instance_plan_cache_max_size").Check(testkit.Rows("2147483648"))
	tk.MustExecToErr("set global tidb_instance_plan_cache_max_size = 2.5GiB")
	tk.MustQuery("select @@global.tidb_instance_plan_cache_max_size").Check(testkit.Rows("2147483648"))
	tk.MustQuery("select @@global.tidb_instance_plan_cache_reserved_percentage").Check(testkit.Rows("0.1"))
	tk.MustExec(`set global tidb_instance_plan_cache_reserved_percentage=1.1`)
	tk.MustQuery("select @@global.tidb_instance_plan_cache_reserved_percentage").Check(testkit.Rows("1"))
	tk.MustExec(`set global tidb_instance_plan_cache_reserved_percentage=-0.1`)
	tk.MustQuery("select @@global.tidb_instance_plan_cache_reserved_percentage").Check(testkit.Rows("0"))
	tk.MustExec(`set global tidb_instance_plan_cache_reserved_percentage=0.5`)
	tk.MustQuery("select @@global.tidb_instance_plan_cache_reserved_percentage").Check(testkit.Rows("0.5"))

	// test variables for cost model ver2
	tk.MustQuery("select @@tidb_cost_model_version").Check(testkit.Rows(fmt.Sprintf("%v", vardef.DefTiDBCostModelVer)))
	tk.MustExec("set tidb_cost_model_version=3")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_cost_model_version value: '3'"))
	tk.MustExec("set tidb_cost_model_version=0")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_cost_model_version value: '0'"))
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustQuery("select @@tidb_cost_model_version").Check(testkit.Rows("2"))

	tk.MustQuery("select @@tidb_enable_analyze_snapshot").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_analyze_snapshot = 1")
	tk.MustQuery("select @@global.tidb_enable_analyze_snapshot").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_analyze_snapshot = 0")
	tk.MustQuery("select @@global.tidb_enable_analyze_snapshot").Check(testkit.Rows("0"))
	tk.MustExec("set session tidb_enable_analyze_snapshot = 1")
	tk.MustQuery("select @@session.tidb_enable_analyze_snapshot").Check(testkit.Rows("1"))
	tk.MustExec("set session tidb_enable_analyze_snapshot = 0")
	tk.MustQuery("select @@session.tidb_enable_analyze_snapshot").Check(testkit.Rows("0"))

	// test variables `init_connect'
	tk.MustGetErrCode("set global init_connect = '-1'", mysql.ErrWrongTypeForVar)
	tk.MustGetErrCode("set global init_connect = 'invalidstring'", mysql.ErrWrongTypeForVar)
	tk.MustExec("set global init_connect = 'select now(); select timestamp()'")

	// test variable 'tidb_session_plan_cache_size'
	// global scope
	tk.MustQuery("select @@global.tidb_session_plan_cache_size").Check(testkit.Rows("100")) // default value
	tk.MustExec("set global tidb_session_plan_cache_size = 1")
	tk.MustQuery("select @@global.tidb_session_plan_cache_size").Check(testkit.Rows("1"))
	// session scope
	tk.MustQuery("select @@session.tidb_session_plan_cache_size").Check(testkit.Rows("100")) // default value
	tk.MustExec("set session tidb_session_plan_cache_size = 1")
	tk.MustQuery("select @@session.tidb_session_plan_cache_size").Check(testkit.Rows("1"))

	// test variable 'foreign_key_checks'
	// global scope
	tk.MustQuery("select @@global.foreign_key_checks").Check(testkit.Rows("1")) // default value
	tk.MustExec("set global foreign_key_checks = 0")
	tk.MustQuery("select @@global.foreign_key_checks").Check(testkit.Rows("0"))
	// session scope
	tk.MustQuery("select @@session.foreign_key_checks").Check(testkit.Rows("1")) // default value
	tk.MustExec("set session foreign_key_checks = 0")
	tk.MustQuery("select @@session.foreign_key_checks").Check(testkit.Rows("0"))

	// test variable 'tidb_enable_foreign_key'
	// global scope
	tk.MustQuery("select @@global.tidb_enable_foreign_key").Check(testkit.Rows("1")) // default value
	tk.MustExec("set global tidb_enable_foreign_key = 0")
	tk.MustQuery("select @@global.tidb_enable_foreign_key").Check(testkit.Rows("0"))

	// test variable 'tidb_opt_force_inline_cte'
	tk.MustQuery("select @@session.tidb_opt_force_inline_cte").Check(testkit.Rows("0")) // default value is 0
	tk.MustExec("set session tidb_opt_force_inline_cte=1")
	tk.MustQuery("select @@session.tidb_opt_force_inline_cte").Check(testkit.Rows("1"))
	tk.MustQuery("select @@global.tidb_opt_force_inline_cte").Check(testkit.Rows("0")) // default value is 0
	tk.MustExec("set global tidb_opt_force_inline_cte=1")
	tk.MustQuery("select @@global.tidb_opt_force_inline_cte").Check(testkit.Rows("1"))

	// test tidb_auto_analyze_partition_batch_size
	tk.MustQuery("select @@global.tidb_auto_analyze_partition_batch_size").Check(testkit.Rows("8192")) // default value is 8192
	tk.MustExec("set global tidb_auto_analyze_partition_batch_size = 2")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1681 Updating 'tidb_auto_analyze_partition_batch_size' is deprecated. It will be made read-only in a future release."))
	tk.MustQuery("select @@global.tidb_auto_analyze_partition_batch_size").Check(testkit.Rows("2"))
	tk.MustExec("set global tidb_auto_analyze_partition_batch_size = 0")
	tk.MustQuery("select @@global.tidb_auto_analyze_partition_batch_size").Check(testkit.Rows("1")) // min value is 1
	tk.MustExec("set global tidb_auto_analyze_partition_batch_size = 9999")
	tk.MustQuery("select @@global.tidb_auto_analyze_partition_batch_size").Check(testkit.Rows("8192")) // max value is 8192

	// test variable 'tidb_opt_prefix_index_single_scan'
	// global scope
	tk.MustQuery("select @@global.tidb_opt_prefix_index_single_scan").Check(testkit.Rows("1")) // default value
	tk.MustExec("set global tidb_opt_prefix_index_single_scan = 0")
	tk.MustQuery("select @@global.tidb_opt_prefix_index_single_scan").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_opt_prefix_index_single_scan = 1")
	tk.MustQuery("select @@global.tidb_opt_prefix_index_single_scan").Check(testkit.Rows("1"))
	// session scope
	tk.MustQuery("select @@session.tidb_opt_prefix_index_single_scan").Check(testkit.Rows("1")) // default value
	tk.MustExec("set session tidb_opt_prefix_index_single_scan = 0")
	tk.MustQuery("select @@session.tidb_opt_prefix_index_single_scan").Check(testkit.Rows("0"))
	tk.MustExec("set session tidb_opt_prefix_index_single_scan = 1")
	tk.MustQuery("select @@session.tidb_opt_prefix_index_single_scan").Check(testkit.Rows("1"))

	// test tidb_opt_range_max_size
	tk.MustQuery("select @@tidb_opt_range_max_size").Check(testkit.Rows("67108864"))
	tk.MustExec("set global tidb_opt_range_max_size = -1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_opt_range_max_size value: '-1'"))
	tk.MustQuery("select @@global.tidb_opt_range_max_size").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_opt_range_max_size = 1048576")
	tk.MustQuery("select @@global.tidb_opt_range_max_size").Check(testkit.Rows("1048576"))
	tk.MustExec("set session tidb_opt_range_max_size = 2097152")
	tk.MustQuery("select @@session.tidb_opt_range_max_size").Check(testkit.Rows("2097152"))

	// test for password validation
	tk.MustQuery("SELECT @@GLOBAL.validate_password.enable").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT @@GLOBAL.validate_password.length").Check(testkit.Rows("8"))
	tk.MustExec("SET GLOBAL validate_password.length = 3")
	tk.MustQuery("SELECT @@GLOBAL.validate_password.length").Check(testkit.Rows("4"))
	tk.MustExec("SET GLOBAL validate_password.mixed_case_count = 2")
	tk.MustQuery("SELECT @@GLOBAL.validate_password.length").Check(testkit.Rows("6"))

	// test tidb_cdc_write_source
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().CDCWriteSource)
	tk.MustQuery("select @@tidb_cdc_write_source").Check(testkit.Rows("0"))
	tk.MustExec("set @@session.tidb_cdc_write_source = 2")
	tk.MustQuery("select @@tidb_cdc_write_source").Check(testkit.Rows("2"))
	require.Equal(t, uint64(2), tk.Session().GetSessionVars().CDCWriteSource)
	tk.MustExec("set @@session.tidb_cdc_write_source = 0")
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().CDCWriteSource)

	tk.MustQuery("select @@session.tidb_analyze_skip_column_types").Check(testkit.Rows("json,blob,mediumblob,longblob,mediumtext,longtext"))
	tk.MustExec("set @@session.tidb_analyze_skip_column_types = 'json, text, blob'")
	tk.MustQuery("select @@session.tidb_analyze_skip_column_types").Check(testkit.Rows("json,text,blob"))
	tk.MustExec("set @@session.tidb_analyze_skip_column_types = ''")
	tk.MustQuery("select @@session.tidb_analyze_skip_column_types").Check(testkit.Rows(""))
	tk.MustGetErrMsg("set @@session.tidb_analyze_skip_column_types = 'int,json'", "[variable:1231]Variable 'tidb_analyze_skip_column_types' can't be set to the value of 'int,json'")

	tk.MustQuery("select @@global.tidb_analyze_skip_column_types").Check(testkit.Rows("json,blob,mediumblob,longblob,mediumtext,longtext"))
	tk.MustExec("set @@global.tidb_analyze_skip_column_types = 'json, text, blob'")
	tk.MustQuery("select @@global.tidb_analyze_skip_column_types").Check(testkit.Rows("json,text,blob"))
	tk.MustExec("set @@global.tidb_analyze_skip_column_types = ''")
	tk.MustQuery("select @@global.tidb_analyze_skip_column_types").Check(testkit.Rows(""))
	tk.MustGetErrMsg("set @@global.tidb_analyze_skip_column_types = 'int,json'", "[variable:1231]Variable 'tidb_analyze_skip_column_types' can't be set to the value of 'int,json'")

	// test tidb_skip_missing_partition_stats
	// global scope
	tk.MustQuery("select @@global.tidb_skip_missing_partition_stats").Check(testkit.Rows("1")) // default value
	tk.MustExec("set global tidb_skip_missing_partition_stats = 0")
	tk.MustQuery("select @@global.tidb_skip_missing_partition_stats").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_skip_missing_partition_stats = 1")
	tk.MustQuery("select @@global.tidb_skip_missing_partition_stats").Check(testkit.Rows("1"))
	// session scope
	tk.MustQuery("select @@session.tidb_skip_missing_partition_stats").Check(testkit.Rows("1")) // default value
	tk.MustExec("set session tidb_skip_missing_partition_stats = 0")
	tk.MustQuery("select @@session.tidb_skip_missing_partition_stats").Check(testkit.Rows("0"))
	tk.MustExec("set session tidb_skip_missing_partition_stats = 1")
	tk.MustQuery("select @@session.tidb_skip_missing_partition_stats").Check(testkit.Rows("1"))

	// test tidb_schema_version_cache_limit
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("16"))
	tk.MustExec("set @@global.tidb_schema_version_cache_limit=64;")
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("64"))
	tk.MustExec("set @@global.tidb_schema_version_cache_limit=2;")
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("2"))
	tk.MustExec("set @@global.tidb_schema_version_cache_limit=256;")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_schema_version_cache_limit value: '256'"))
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("255"))
	tk.MustExec("set @@global.tidb_schema_version_cache_limit=0;")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_schema_version_cache_limit value: '0'"))
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("2"))
	tk.MustGetErrMsg("set @@global.tidb_schema_version_cache_limit='x';", "[variable:1232]Incorrect argument type to variable 'tidb_schema_version_cache_limit'")
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("2"))
	tk.MustExec("set @@global.tidb_schema_version_cache_limit=64;")
	tk.MustQuery("select @@global.tidb_schema_version_cache_limit").Check(testkit.Rows("64"))

	// test tidb_idle_transaction_timeout
	tk.MustQuery("select @@session.tidb_idle_transaction_timeout").Check(testkit.Rows("0"))
	tk.MustExec("SET SESSION tidb_idle_transaction_timeout = 2")
	tk.MustQuery("select @@session.tidb_idle_transaction_timeout").Check(testkit.Rows("2"))
	tk.MustGetErrMsg("SET SESSION tidb_idle_transaction_timeout='x';", "[variable:1232]Incorrect argument type to variable 'tidb_idle_transaction_timeout'")
	tk.MustExec("SET SESSION tidb_idle_transaction_timeout=31536001;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_idle_transaction_timeout value: '31536001'"))
	tk.MustQuery("select @@session.tidb_idle_transaction_timeout").Check(testkit.Rows("31536000"))
	tk.MustExec("SET SESSION tidb_idle_transaction_timeout = 0")
	tk.MustQuery("select @@session.tidb_idle_transaction_timeout").Check(testkit.Rows("0"))
	tk.MustExec("SET SESSION tidb_idle_transaction_timeout=31536000;")
	tk.MustQuery("select @@session.tidb_idle_transaction_timeout").Check(testkit.Rows("31536000"))
	tk.MustQuery("select @@global.tidb_idle_transaction_timeout").Check(testkit.Rows("0"))
	tk.MustExec("SET GLOBAL tidb_idle_transaction_timeout = 1")
	tk.MustQuery("select @@global.tidb_idle_transaction_timeout").Check(testkit.Rows("1"))
	tk.MustGetErrMsg("SET GLOBAL tidb_idle_transaction_timeout='x';", "[variable:1232]Incorrect argument type to variable 'tidb_idle_transaction_timeout'")
	tk.MustExec("SET GLOBAL tidb_idle_transaction_timeout=31536001;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_idle_transaction_timeout value: '31536001'"))
	tk.MustQuery("select @@global.tidb_idle_transaction_timeout").Check(testkit.Rows("31536000"))
	tk.MustExec("SET GLOBAL tidb_idle_transaction_timeout = 0")
	tk.MustQuery("select @@global.tidb_idle_transaction_timeout").Check(testkit.Rows("0"))
	tk.MustExec("SET GLOBAL tidb_idle_transaction_timeout=31536000;")
	tk.MustQuery("select @@global.tidb_idle_transaction_timeout").Check(testkit.Rows("31536000"))

	// test tidb_txn_entry_size_limit
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit").Check(testkit.Rows("0"))
	tk.MustExec("set session tidb_txn_entry_size_limit = 1024")
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit").Check(testkit.Rows("1024"))
	tk.MustExec("set session tidb_txn_entry_size_limit = 125829120")
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit").Check(testkit.Rows("125829120"))

	tk.MustGetErrMsg("set session tidb_txn_entry_size_limit = 'x'", "[variable:1232]Incorrect argument type to variable 'tidb_txn_entry_size_limit'")
	tk.MustGetErrMsg("set session tidb_txn_entry_size_limit = 18446744073709551616", "[variable:1232]Incorrect argument type to variable 'tidb_txn_entry_size_limit'")

	tk.MustExec("set session tidb_txn_entry_size_limit = 125829121")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_txn_entry_size_limit value: '125829121'"))
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit").Check(testkit.Rows("125829120"))
	tk.MustExec("set session tidb_txn_entry_size_limit = -1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_txn_entry_size_limit value: '-1'"))
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit").Check(testkit.Rows("0"))

	tk.MustExec("set session tidb_txn_entry_size_limit = 2048")
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit, @@global.tidb_txn_entry_size_limit").Check(testkit.Rows("2048 0"))
	tk.MustExec("set global tidb_txn_entry_size_limit = 4096")
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit, @@global.tidb_txn_entry_size_limit").Check(testkit.Rows("2048 4096"))
	tk.MustExec("set global tidb_txn_entry_size_limit = 0")
	tk.MustQuery("select @@session.tidb_txn_entry_size_limit, @@global.tidb_txn_entry_size_limit").Check(testkit.Rows("2048 0"))

	// test for tidb_opt_projection_push_down
	tk.MustQuery("select @@session.tidb_opt_projection_push_down, @@global.tidb_opt_projection_push_down").Check(testkit.Rows("1 1"))
	tk.MustExec("set global tidb_opt_projection_push_down = 'OFF'")
	tk.MustQuery("select @@session.tidb_opt_projection_push_down, @@global.tidb_opt_projection_push_down").Check(testkit.Rows("1 0"))
	tk.MustExec("set session tidb_opt_projection_push_down = 'OFF'")
	tk.MustQuery("select @@session.tidb_opt_projection_push_down, @@global.tidb_opt_projection_push_down").Check(testkit.Rows("0 0"))
	tk.MustExec("set global tidb_opt_projection_push_down = 'on'")
	tk.MustQuery("select @@session.tidb_opt_projection_push_down, @@global.tidb_opt_projection_push_down").Check(testkit.Rows("0 1"))
	require.Error(t, tk.ExecToErr("set global tidb_opt_projection_push_down = 'UNKNOWN'"))
}

func TestSetCollationAndCharset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx := tk.Session().(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()

	cases := []struct {
		charset         string
		collation       string
		expectCharset   string
		expectCollation string
	}{
		{vardef.CharacterSetConnection, vardef.CollationConnection, "utf8", "utf8_bin"},
		{vardef.CharsetDatabase, vardef.CollationDatabase, "utf8", "utf8_bin"},
		{vardef.CharacterSetServer, vardef.CollationServer, "utf8", "utf8_bin"},
	}

	for _, c := range cases {
		tk.MustExec(fmt.Sprintf("set %s = %s;", c.charset, c.expectCharset))
		sVar, ok := sessionVars.GetSystemVar(c.charset)
		require.True(t, ok)
		require.Equal(t, c.expectCharset, sVar)
		sVar, ok = sessionVars.GetSystemVar(c.collation)
		require.True(t, ok)
		require.Equal(t, c.expectCollation, sVar)
	}

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx = tk.Session().(sessionctx.Context)
	sessionVars = ctx.GetSessionVars()

	for _, c := range cases {
		tk.MustExec(fmt.Sprintf("set %s = %s;", c.collation, c.expectCollation))
		sVar, ok := sessionVars.GetSystemVar(c.charset)
		require.True(t, ok)
		require.Equal(t, c.expectCharset, sVar)
		sVar, ok = sessionVars.GetSystemVar(c.collation)
		require.True(t, ok)
		require.Equal(t, c.expectCollation, sVar)
	}
}

func TestValidateSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	err := tk.ExecToErr("set global tidb_analyze_distsql_scan_concurrency='fff';")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set global tidb_analyze_distsql_scan_concurrency=-2;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_analyze_distsql_scan_concurrency value: '-2'"))

	err = tk.ExecToErr("set @@tidb_analyze_distsql_scan_concurrency='fff';")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@tidb_analyze_distsql_scan_concurrency=-2;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_analyze_distsql_scan_concurrency value: '-2'"))

	err = tk.ExecToErr("set global tidb_distsql_scan_concurrency='fff';")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set global tidb_distsql_scan_concurrency=-2;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_distsql_scan_concurrency value: '-2'"))

	err = tk.ExecToErr("set @@tidb_distsql_scan_concurrency='fff';")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@tidb_distsql_scan_concurrency=-2;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_distsql_scan_concurrency value: '-2'"))

	err = tk.ExecToErr("set @@tidb_batch_delete='ok';")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@tidb_batch_delete='On';")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_batch_delete='oFf';")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("0"))
	tk.MustExec("set @@tidb_batch_delete=1;")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_batch_delete=0;")
	tk.MustQuery("select @@tidb_batch_delete;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_opt_agg_push_down=off;")
	tk.MustQuery("select @@tidb_opt_agg_push_down;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_constraint_check_in_place=on;")
	tk.MustQuery("select @@tidb_constraint_check_in_place;").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_general_log=0;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows(fmt.Sprintf("Warning %d modifying tidb_general_log will require SET GLOBAL in a future version of TiDB", errno.ErrInstanceScope)))
	tk.MustQuery("select @@tidb_general_log;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_pprof_sql_cpu=1;")
	tk.MustQuery("select @@tidb_pprof_sql_cpu;").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_pprof_sql_cpu=0;")
	tk.MustQuery("select @@tidb_pprof_sql_cpu;").Check(testkit.Rows("0"))

	err = tk.ExecToErr("set @@tidb_batch_delete=3;")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result := tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("4"))

	err = tk.ExecToErr("set @@group_concat_max_len = 18446744073709551616")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	// Test illegal type
	err = tk.ExecToErr("set @@group_concat_max_len='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@default_week_format=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect default_week_format value: '-1'"))
	result = tk.MustQuery("select @@default_week_format;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@default_week_format=9")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect default_week_format value: '9'"))
	result = tk.MustQuery("select @@default_week_format;")
	result.Check(testkit.Rows("7"))

	err = tk.ExecToErr("set @@error_count = 0")
	require.True(t, terror.ErrorEqual(err, variable.ErrIncorrectScope), fmt.Sprintf("err %v", err))

	err = tk.ExecToErr("set @@warning_count = 0")
	require.True(t, terror.ErrorEqual(err, variable.ErrIncorrectScope), fmt.Sprintf("err %v", err))

	tk.MustExec("set time_zone='SySTeM'")
	result = tk.MustQuery("select @@time_zone;")
	result.Check(testkit.Rows("SYSTEM"))

	// The following cases test value out of range and illegal type when setting system variables.
	// See https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html for more details.
	tk.MustExec("set @@global.max_connections=100001")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '100001'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("100000"))

	// "max_connections == 0" means there is no limitation on the number of connections.
	tk.MustExec("set @@global.max_connections=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '-1'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("0"))

	err = tk.ExecToErr("set @@global.max_connections='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@global.thread_pool_size=65")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect thread_pool_size value: '65'"))
	result = tk.MustQuery("select @@global.thread_pool_size;")
	result.Check(testkit.Rows("64"))

	tk.MustExec("set @@global.thread_pool_size=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect thread_pool_size value: '-1'"))
	result = tk.MustQuery("select @@global.thread_pool_size;")
	result.Check(testkit.Rows("1"))

	err = tk.ExecToErr("set @@global.thread_pool_size='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@global.max_allowed_packet=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_allowed_packet value: '-1'"))
	result = tk.MustQuery("select @@global.max_allowed_packet;")
	result.Check(testkit.Rows("1024"))

	err = tk.ExecToErr("set @@global.max_allowed_packet='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	err = tk.ExecToErr("set @@max_allowed_packet=default")
	require.True(t, terror.ErrorEqual(err, variable.ErrReadOnly))

	tk.MustExec("set @@global.max_connect_errors=18446744073709551615")

	tk.MustExec("set @@global.max_connect_errors=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connect_errors value: '-1'"))
	result = tk.MustQuery("select @@global.max_connect_errors;")
	result.Check(testkit.Rows("1"))

	err = tk.ExecToErr("set @@global.max_connect_errors=18446744073709551616")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@global.max_connections=100001")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '100001'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("100000"))

	tk.MustExec("set @@global.max_connections=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_connections value: '-1'"))
	result = tk.MustQuery("select @@global.max_connections;")
	result.Check(testkit.Rows("0"))

	err = tk.ExecToErr("set @@global.max_connections='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@max_sort_length=1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '1'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Rows("4"))

	tk.MustExec("set @@max_sort_length=-100")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '-100'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Rows("4"))

	tk.MustExec("set @@max_sort_length=8388609")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect max_sort_length value: '8388609'"))
	result = tk.MustQuery("select @@max_sort_length;")
	result.Check(testkit.Rows("8388608"))

	err = tk.ExecToErr("set @@max_sort_length='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@global.table_definition_cache=399")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect table_definition_cache value: '399'"))
	result = tk.MustQuery("select @@global.table_definition_cache;")
	result.Check(testkit.Rows("400"))

	tk.MustExec("set @@global.table_definition_cache=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect table_definition_cache value: '-1'"))
	result = tk.MustQuery("select @@global.table_definition_cache;")
	result.Check(testkit.Rows("400"))

	tk.MustExec("set @@global.table_definition_cache=524289")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect table_definition_cache value: '524289'"))
	result = tk.MustQuery("select @@global.table_definition_cache;")
	result.Check(testkit.Rows("524288"))

	err = tk.ExecToErr("set @@global.table_definition_cache='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@old_passwords=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect old_passwords value: '-1'"))
	result = tk.MustQuery("select @@old_passwords;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@old_passwords=3")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect old_passwords value: '3'"))
	result = tk.MustQuery("select @@old_passwords;")
	result.Check(testkit.Rows("2"))

	err = tk.ExecToErr("set @@old_passwords='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@tmp_table_size=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tmp_table_size value: '-1'"))
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("1024"))

	tk.MustExec("set @@tmp_table_size=1020")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tmp_table_size value: '1020'"))
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("1024"))

	tk.MustExec("set @@tmp_table_size=167772161")
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("167772161"))

	tk.MustExec("set @@tmp_table_size=18446744073709551615")
	result = tk.MustQuery("select @@tmp_table_size;")
	result.Check(testkit.Rows("18446744073709551615"))

	err = tk.ExecToErr("set @@tmp_table_size=18446744073709551616")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	err = tk.ExecToErr("set @@tmp_table_size='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@tidb_tmp_table_max_size=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tmp_table_max_size value: '-1'"))
	result = tk.MustQuery("select @@tidb_tmp_table_max_size;")
	result.Check(testkit.Rows("1048576"))

	tk.MustExec("set @@tidb_tmp_table_max_size=1048575")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tmp_table_max_size value: '1048575'"))
	result = tk.MustQuery("select @@tidb_tmp_table_max_size;")
	result.Check(testkit.Rows("1048576"))

	tk.MustExec("set @@tidb_tmp_table_max_size=167772161")
	result = tk.MustQuery("select @@tidb_tmp_table_max_size;")
	result.Check(testkit.Rows("167772161"))

	tk.MustExec("set @@tidb_tmp_table_max_size=137438953472")
	result = tk.MustQuery("select @@tidb_tmp_table_max_size;")
	result.Check(testkit.Rows("137438953472"))

	tk.MustExec("set @@tidb_tmp_table_max_size=137438953473")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tmp_table_max_size value: '137438953473'"))
	result = tk.MustQuery("select @@tidb_tmp_table_max_size;")
	result.Check(testkit.Rows("137438953472"))

	err = tk.ExecToErr("set @@tidb_tmp_table_max_size='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar))

	tk.MustExec("set @@global.connect_timeout=1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect connect_timeout value: '1'"))
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Rows("2"))

	tk.MustExec("set @@global.connect_timeout=31536000")
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Rows("31536000"))

	tk.MustExec("set @@global.connect_timeout=31536001")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect connect_timeout value: '31536001'"))
	result = tk.MustQuery("select @@global.connect_timeout;")
	result.Check(testkit.Rows("31536000"))

	result = tk.MustQuery("select @@sql_select_limit;")
	result.Check(testkit.Rows("18446744073709551615"))
	tk.MustExec("set @@sql_select_limit=default")
	result = tk.MustQuery("select @@sql_select_limit;")
	result.Check(testkit.Rows("18446744073709551615"))

	tk.MustExec("set @@sql_auto_is_null=00")
	result = tk.MustQuery("select @@sql_auto_is_null;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@sql_warnings=001")
	result = tk.MustQuery("select @@sql_warnings;")
	result.Check(testkit.Rows("1"))

	tk.MustExec("set @@sql_warnings=000")
	result = tk.MustQuery("select @@sql_warnings;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@global.super_read_only=-0")
	result = tk.MustQuery("select @@global.super_read_only;")
	result.Check(testkit.Rows("0"))

	err = tk.ExecToErr("set @@global.super_read_only=-1")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@global.innodb_status_output_locks=-1")
	result = tk.MustQuery("select @@global.innodb_status_output_locks;")
	result.Check(testkit.Rows("1"))

	tk.MustExec("set @@global.innodb_ft_enable_stopword=0000000")
	result = tk.MustQuery("select @@global.innodb_ft_enable_stopword;")
	result.Check(testkit.Rows("0"))

	tk.MustExec("set @@global.innodb_stats_on_metadata=1")
	result = tk.MustQuery("select @@global.innodb_stats_on_metadata;")
	result.Check(testkit.Rows("1"))

	tk.MustExec("set @@global.innodb_file_per_table=-50")
	result = tk.MustQuery("select @@global.innodb_file_per_table;")
	result.Check(testkit.Rows("1"))

	err = tk.ExecToErr("set @@global.innodb_ft_enable_stopword=2")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	tk.MustContainErrMsg("set @@query_cache_type=0", "[variable:1193]Unknown system variable 'query_cache_type'")
	tk.MustContainErrMsg("select @@query_cache_type;", "[variable:1193]Unknown system variable 'query_cache_type'")

	tk.MustContainErrMsg("set @@query_cache_type=2", "[variable:1193]Unknown system variable 'query_cache_type'")
	tk.MustContainErrMsg("select @@query_cache_type;", "[variable:1193]Unknown system variable 'query_cache_type'")

	tk.MustExec("set @@global.sync_binlog=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect sync_binlog value: '-1'"))

	tk.MustExec("set @@global.sync_binlog=4294967299")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect sync_binlog value: '4294967299'"))

	tk.MustExec("set @@global.flush_time=31536001")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect flush_time value: '31536001'"))

	tk.MustExec("set @@global.interactive_timeout=31536001")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect interactive_timeout value: '31536001'"))

	tk.MustExec("set @@global.innodb_commit_concurrency = -1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_commit_concurrency value: '-1'"))

	tk.MustExec("set @@global.innodb_commit_concurrency = 1001")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_commit_concurrency value: '1001'"))

	tk.MustExec("set @@global.innodb_fast_shutdown = -1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_fast_shutdown value: '-1'"))

	tk.MustExec("set @@global.innodb_fast_shutdown = 3")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_fast_shutdown value: '3'"))

	tk.MustExec("set @@global.innodb_lock_wait_timeout = 0")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '0'"))

	tk.MustExec("set @@global.innodb_lock_wait_timeout = 1073741825")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '1073741825'"))

	tk.MustExec("set @@innodb_lock_wait_timeout = 0")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '0'"))

	tk.MustExec("set @@innodb_lock_wait_timeout = 1073741825")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect innodb_lock_wait_timeout value: '1073741825'"))

	tk.MustExec("set @@global.validate_password.number_count=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect validate_password.number_count value: '-1'"))

	tk.MustExec("set @@global.validate_password.length=-1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect validate_password.length value: '-1'"))

	err = tk.ExecToErr("set @@tx_isolation=''")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	err = tk.ExecToErr("set global tx_isolation=''")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	err = tk.ExecToErr("set @@transaction_isolation=''")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	err = tk.ExecToErr("set global transaction_isolation=''")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	err = tk.ExecToErr("set global tx_isolation='REPEATABLE-READ1'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongValueForVar), fmt.Sprintf("err %v", err))

	tk.MustExec("set @@tx_isolation='READ-COMMITTED'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("set @@tx_isolation='read-COMMITTED'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Rows("READ-COMMITTED"))

	tk.MustExec("set @@tx_isolation='REPEATABLE-READ'")
	result = tk.MustQuery("select @@tx_isolation;")
	result.Check(testkit.Rows("REPEATABLE-READ"))

	tk.MustExec("SET GLOBAL tidb_skip_isolation_level_check = 0")
	tk.MustExec("SET SESSION tidb_skip_isolation_level_check = 0")
	err = tk.ExecToErr("set @@tx_isolation='SERIALIZABLE'")
	require.True(t, terror.ErrorEqual(err, variable.ErrUnsupportedIsolationLevel), fmt.Sprintf("err %v", err))

	tk.MustExec("set global allow_auto_random_explicit_insert=on;")
	tk.MustQuery("select @@global.allow_auto_random_explicit_insert;").Check(testkit.Rows("1"))
}

func TestSetConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test default value
	tk.MustQuery("select @@tidb_executor_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.DefExecutorConcurrency)))

	tk.MustQuery("select @@tidb_index_lookup_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_index_lookup_join_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_hash_join_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_hashagg_partial_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_hashagg_final_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_window_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_streamagg_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.DefTiDBStreamAggConcurrency)))
	tk.MustQuery("select @@tidb_projection_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.ConcurrencyUnset)))
	tk.MustQuery("select @@tidb_distsql_scan_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.DefDistSQLScanConcurrency)))

	tk.MustQuery("select @@tidb_index_serial_scan_concurrency;").Check(testkit.Rows(strconv.Itoa(vardef.DefIndexSerialScanConcurrency)))

	vars := tk.Session().GetSessionVars()
	require.Equal(t, vardef.DefExecutorConcurrency, vars.ExecutorConcurrency)
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupJoinConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashJoinConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashAggPartialConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashAggFinalConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.WindowConcurrency())
	require.Equal(t, vardef.DefTiDBStreamAggConcurrency, vars.StreamAggConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.ProjectionConcurrency())
	require.Equal(t, vardef.DefDistSQLScanConcurrency, vars.DistSQLScanConcurrency())

	require.Equal(t, vardef.DefIndexSerialScanConcurrency, vars.IndexSerialScanConcurrency())

	// test setting deprecated variables
	warnTpl := "Warning 1287 '%s' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead"

	checkSet := func(v string) {
		tk.MustExec(fmt.Sprintf("set @@%s=1;", v))
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", fmt.Sprintf(warnTpl, v)))
		tk.MustQuery(fmt.Sprintf("select @@%s;", v)).Check(testkit.Rows("1"))
	}

	checkSet(vardef.TiDBIndexLookupConcurrency)
	require.Equal(t, 1, vars.IndexLookupConcurrency())

	checkSet(vardef.TiDBIndexLookupJoinConcurrency)
	require.Equal(t, 1, vars.IndexLookupJoinConcurrency())

	checkSet(vardef.TiDBHashJoinConcurrency)
	require.Equal(t, 1, vars.HashJoinConcurrency())

	checkSet(vardef.TiDBHashAggPartialConcurrency)
	require.Equal(t, 1, vars.HashAggPartialConcurrency())

	checkSet(vardef.TiDBHashAggFinalConcurrency)
	require.Equal(t, 1, vars.HashAggFinalConcurrency())

	checkSet(vardef.TiDBProjectionConcurrency)
	require.Equal(t, 1, vars.ProjectionConcurrency())

	checkSet(vardef.TiDBWindowConcurrency)
	require.Equal(t, 1, vars.WindowConcurrency())

	checkSet(vardef.TiDBStreamAggConcurrency)
	require.Equal(t, 1, vars.StreamAggConcurrency())

	tk.MustExec(fmt.Sprintf("set @@%s=1;", vardef.TiDBDistSQLScanConcurrency))
	tk.MustQuery(fmt.Sprintf("select @@%s;", vardef.TiDBDistSQLScanConcurrency)).Check(testkit.Rows("1"))
	require.Equal(t, 1, vars.DistSQLScanConcurrency())

	tk.MustExec("set @@tidb_index_serial_scan_concurrency=4")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select @@tidb_index_serial_scan_concurrency;").Check(testkit.Rows("4"))
	require.Equal(t, 4, vars.IndexSerialScanConcurrency())

	// test setting deprecated value unset
	tk.MustExec("set @@tidb_index_lookup_concurrency=-1;")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=-1;")
	tk.MustExec("set @@tidb_hash_join_concurrency=-1;")
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=-1;")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=-1;")
	tk.MustExec("set @@tidb_window_concurrency=-1;")
	tk.MustExec("set @@tidb_streamagg_concurrency=-1;")
	tk.MustExec("set @@tidb_projection_concurrency=-1;")

	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.IndexLookupJoinConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashJoinConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashAggPartialConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.HashAggFinalConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.WindowConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.StreamAggConcurrency())
	require.Equal(t, vardef.DefExecutorConcurrency, vars.ProjectionConcurrency())

	tk.MustExec("set @@tidb_executor_concurrency=-1;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_executor_concurrency value: '-1'"))
	require.Equal(t, 1, vars.ExecutorConcurrency)
}

func TestEnableNoopFunctionsVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	defer func() {
		// Ensure global settings are reset.
		tk.MustExec("SET GLOBAL tx_read_only = 0")
		tk.MustExec("SET GLOBAL transaction_read_only = 0")
		tk.MustExec("SET GLOBAL read_only = 0")
		tk.MustExec("SET GLOBAL super_read_only = 0")
		tk.MustExec("SET GLOBAL offline_mode = 0")
		tk.MustExec("SET GLOBAL tidb_enable_noop_functions = 0")
	}()

	// test for tidb_enable_noop_functions
	tk.MustQuery(`select @@global.tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))

	// change session var to 1
	tk.MustExec(`set tidb_enable_noop_functions=1;`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("ON"))
	tk.MustQuery(`select @@global.tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))

	// restore to 0
	tk.MustExec(`set tidb_enable_noop_functions=0;`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))
	tk.MustQuery(`select @@global.tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))

	// set test
	require.Error(t, tk.ExecToErr(`set tidb_enable_noop_functions='abc'`))
	require.Error(t, tk.ExecToErr(`set tidb_enable_noop_functions=11`))
	tk.MustExec(`set tidb_enable_noop_functions="off";`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))
	tk.MustExec(`set tidb_enable_noop_functions="on";`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("ON"))
	tk.MustExec(`set tidb_enable_noop_functions=0;`)
	tk.MustQuery(`select @@tidb_enable_noop_functions;`).Check(testkit.Rows("OFF"))

	err := tk.ExecToErr("SET SESSION tx_read_only = 1")
	require.True(t, terror.ErrorEqual(err, variable.ErrFunctionsNoopImpl), fmt.Sprintf("err %v", err))

	tk.MustExec("SET SESSION tx_read_only = 0")
	tk.MustQuery("select @@session.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.transaction_read_only").Check(testkit.Rows("0"))

	err = tk.ExecToErr("SET GLOBAL tx_read_only = 1") // should fail.
	require.True(t, terror.ErrorEqual(err, variable.ErrFunctionsNoopImpl), fmt.Sprintf("err %v", err))
	tk.MustExec("SET GLOBAL tx_read_only = 0")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Rows("0"))

	err = tk.ExecToErr("SET SESSION transaction_read_only = 1")
	require.True(t, terror.ErrorEqual(err, variable.ErrFunctionsNoopImpl), fmt.Sprintf("err %v", err))
	tk.MustExec("SET SESSION transaction_read_only = 0")
	tk.MustQuery("select @@session.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.transaction_read_only").Check(testkit.Rows("0"))

	// works on SESSION because SESSION tidb_enable_noop_functions=1
	tk.MustExec("SET tidb_enable_noop_functions = 1")
	tk.MustExec("SET SESSION transaction_read_only = 1")
	tk.MustQuery("select @@session.tx_read_only").Check(testkit.Rows("1"))
	tk.MustQuery("select @@session.transaction_read_only").Check(testkit.Rows("1"))

	// fails on GLOBAL because GLOBAL.tidb_enable_noop_functions still=0
	err = tk.ExecToErr("SET GLOBAL transaction_read_only = 1")
	require.True(t, terror.ErrorEqual(err, variable.ErrFunctionsNoopImpl), fmt.Sprintf("err %v", err))
	tk.MustExec("SET GLOBAL tidb_enable_noop_functions = 1")
	// now works
	tk.MustExec("SET GLOBAL transaction_read_only = 1")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Rows("1"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Rows("1"))
	tk.MustExec("SET GLOBAL transaction_read_only = 0")
	tk.MustQuery("select @@global.tx_read_only").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.transaction_read_only").Check(testkit.Rows("0"))

	require.Error(t, tk.ExecToErr("SET tidb_enable_noop_functions = 0")) // fails because transaction_read_only/tx_read_only = 1

	tk.MustExec("SET transaction_read_only = 0")
	tk.MustExec("SET tidb_enable_noop_functions = 0") // now works.

	// setting session doesn't change global, which succeeds because global.transaction_read_only/tx_read_only = 0
	tk.MustExec("SET GLOBAL tidb_enable_noop_functions = 0")

	// but if global.transaction_read_only=1, it would fail
	tk.MustExec("SET GLOBAL tidb_enable_noop_functions = 1")
	tk.MustExec("SET GLOBAL transaction_read_only = 1")
	// fails
	require.Error(t, tk.ExecToErr("SET GLOBAL tidb_enable_noop_functions = 0"))

	// reset for rest of tests.
	tk.MustExec("SET GLOBAL transaction_read_only = 0")
	tk.MustExec("SET GLOBAL tidb_enable_noop_functions = 0")

	tk.MustExec("set global read_only = 0")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("0"))
	tk.MustExec("set global read_only = off")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("0"))
	tk.MustExec("SET global tidb_enable_noop_functions = 1")
	tk.MustExec("set global read_only = 1")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("1"))
	tk.MustExec("set global read_only = on")
	tk.MustQuery("select @@global.read_only;").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("set global read_only = abc"))
}

func TestSetClusterConfig(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	serversInfo := []infoschema.ServerInfo{
		{ServerType: "tidb", Address: "127.0.0.1:1111", StatusAddr: "127.0.0.1:1111"},
		{ServerType: "tidb", Address: "127.0.0.1:2222", StatusAddr: "127.0.0.1:2222"},
		{ServerType: "pd", Address: "127.0.0.1:3333", StatusAddr: "127.0.0.1:3333"},
		{ServerType: "pd", Address: "127.0.0.1:4444", StatusAddr: "127.0.0.1:4444"},
		{ServerType: "tikv", Address: "127.0.0.1:5555", StatusAddr: "127.0.0.1:5555"},
		{ServerType: "tikv", Address: "127.0.0.1:6666", StatusAddr: "127.0.0.1:6666"},
		{ServerType: "tiflash", Address: "127.0.0.1:3933", StatusAddr: "127.0.0.1:7777"},
		{ServerType: "tso", Address: "127.0.0.1:22379", StatusAddr: "127.0.0.1:22379"},
		{ServerType: "scheduling", Address: "127.0.0.1:22479", StatusAddr: "127.0.0.1:22479"},
	}
	var serverInfoErr error
	serverInfoFunc := func(sessionctx.Context) ([]infoschema.ServerInfo, error) {
		return serversInfo, serverInfoErr
	}
	tk.Session().SetValue(executor.TestSetConfigServerInfoKey, serverInfoFunc)

	require.EqualError(t, tk.ExecToErr("set config xxx log.level='info'"), "unknown type xxx")
	require.EqualError(t, tk.ExecToErr("set config tidb log.level='info'"), "TiDB doesn't support to change configs online, please use SQL variables")
	require.EqualError(t, tk.ExecToErr("set config tso log.level='info'"), "tso doesn't support to change configs online")
	require.EqualError(t, tk.ExecToErr("set config scheduling log.level='info'"), "scheduling doesn't support to change configs online")
	require.EqualError(t, tk.ExecToErr("set config '127.0.0.1:1111' log.level='info'"), "TiDB doesn't support to change configs online, please use SQL variables")
	require.EqualError(t, tk.ExecToErr("set config '127.a.b.c:1234' log.level='info'"), "invalid instance 127.a.b.c:1234")                          // name doesn't resolve.
	require.EqualError(t, tk.ExecToErr("set config 'example.com:1111' log.level='info'"), "instance example.com:1111 is not found in this cluster") // name resolves.
	require.EqualError(t, tk.ExecToErr("set config tikv log.level=null"), "can't set config to null")
	require.EqualError(t, tk.ExecToErr("set config '1.1.1.1:1111' log.level='info'"), "instance 1.1.1.1:1111 is not found in this cluster")
	require.EqualError(t, tk.ExecToErr("set config tikv `raftstore.max-peer-down-duration`=DEFAULT"), "Unknown DEFAULT for SET CONFIG")
	require.ErrorContains(t, tk.ExecToErr("set config tiflash `server.snap-max-write-bytes-per-sec`='500MB'"), "This command can only change config items begin with 'raftstore-proxy'")

	httpCnt := 0
	tk.Session().SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		httpCnt++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(nil)}, nil
	})
	tk.MustExec("set config tikv log.level='info'")
	require.Equal(t, 2, httpCnt)

	httpCnt = 0
	tk.MustExec("set config '127.0.0.1:5555' log.level='info'")
	require.Equal(t, 1, httpCnt)

	httpCnt = 0
	tk.Session().SetValue(executor.TestSetConfigHTTPHandlerKey, func(req *http.Request) (*http.Response, error) {
		httpCnt++
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		// The `raftstore.` prefix is stripped.
		require.JSONEq(t, `{"server.snap-max-write-bytes-per-sec":"500MB"}`, string(body))
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(nil)}, nil
	})
	tk.MustExec("set config tiflash `raftstore-proxy.server.snap-max-write-bytes-per-sec`='500MB'")
	require.Equal(t, 1, httpCnt)

	httpCnt = 0
	tk.Session().SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("something wrong")
	})
	tk.MustExec("set config tikv log.level='info'")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 something wrong", "Warning 1105 something wrong"))

	tk.Session().SetValue(executor.TestSetConfigHTTPHandlerKey, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewBufferString("WRONG"))}, nil
	})
	tk.MustExec("set config tikv log.level='info'")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 bad request to http://127.0.0.1:5555/config: WRONG", "Warning 1105 bad request to http://127.0.0.1:6666/config: WRONG"))
}

func TestSetClusterConfigJSONData(t *testing.T) {
	var d types.MyDecimal
	require.NoError(t, d.FromFloat64(123.456))
	tyBool := types.NewFieldType(mysql.TypeTiny)
	tyBool.AddFlag(mysql.IsBooleanFlag)
	cases := []struct {
		val    expression.Expression
		result string
		succ   bool
	}{
		{&expression.Constant{Value: types.NewIntDatum(1), RetType: tyBool}, `{"k":true}`, true},
		{&expression.Constant{Value: types.NewIntDatum(0), RetType: tyBool}, `{"k":false}`, true},
		{&expression.Constant{Value: types.NewIntDatum(2333), RetType: types.NewFieldType(mysql.TypeLong)}, `{"k":2333}`, true},
		{&expression.Constant{Value: types.NewFloat64Datum(23.33), RetType: types.NewFieldType(mysql.TypeDouble)}, `{"k":23.33}`, true},
		{&expression.Constant{Value: types.NewStringDatum("abcd"), RetType: types.NewFieldType(mysql.TypeString)}, `{"k":"abcd"}`, true},
		{&expression.Constant{Value: types.NewDecimalDatum(&d), RetType: types.NewFieldType(mysql.TypeNewDecimal)}, `{"k":123.456}`, true},
		{&expression.Constant{Value: types.NewDatum(nil), RetType: types.NewFieldType(mysql.TypeLonglong)}, "", false},
		{&expression.Constant{RetType: types.NewFieldType(mysql.TypeJSON)}, "", false}, // unsupported type
		{nil, "", false},
		{&expression.Constant{Value: types.NewDatum(`["no","no","lz4","lz4","lz4","zstd","zstd"]`), RetType: types.NewFieldType(mysql.TypeString)}, `{"k":"[\"no\",\"no\",\"lz4\",\"lz4\",\"lz4\",\"zstd\",\"zstd\"]"}`, true},
	}

	ctx := mock.NewContext()
	for _, c := range cases {
		result, err := executor.ConvertConfigItem2JSON(ctx, "k", c.val)
		if c.succ {
			require.Equal(t, result, c.result)
		} else {
			require.Error(t, err)
		}
	}
}

func TestSetTopSQLVariables(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/skipLoadSysVarCacheLoop", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/skipLoadSysVarCacheLoop"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@global.tidb_enable_top_sql='On';")
	tk.MustQuery("select @@global.tidb_enable_top_sql;").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_enable_top_sql='off';")
	tk.MustQuery("select @@global.tidb_enable_top_sql;").Check(testkit.Rows("0"))

	tk.MustExec("set @@global.tidb_top_sql_max_time_series_count=20;")
	tk.MustQuery("select @@global.tidb_top_sql_max_time_series_count;").Check(testkit.Rows("20"))
	require.Equal(t, int64(20), topsqlstate.GlobalState.MaxStatementCount.Load())
	err := tk.ExecToErr("set @@global.tidb_top_sql_max_time_series_count='abc';")
	require.EqualError(t, err, "[variable:1232]Incorrect argument type to variable 'tidb_top_sql_max_time_series_count'")
	tk.MustExec("set @@global.tidb_top_sql_max_time_series_count='-1';")
	tk.MustQuery("select @@global.tidb_top_sql_max_time_series_count;").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_top_sql_max_time_series_count='5001';")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_top_sql_max_time_series_count value: '5001'"))
	tk.MustQuery("select @@global.tidb_top_sql_max_time_series_count;").Check(testkit.Rows("5000"))

	tk.MustExec("set @@global.tidb_top_sql_max_time_series_count=20;")
	tk.MustQuery("select @@global.tidb_top_sql_max_time_series_count;").Check(testkit.Rows("20"))
	require.Equal(t, int64(20), topsqlstate.GlobalState.MaxStatementCount.Load())

	tk.MustExec("set @@global.tidb_top_sql_max_meta_count=10000;")
	tk.MustQuery("select @@global.tidb_top_sql_max_meta_count;").Check(testkit.Rows("10000"))
	require.Equal(t, int64(10000), topsqlstate.GlobalState.MaxCollect.Load())
	err = tk.ExecToErr("set @@global.tidb_top_sql_max_meta_count='abc';")
	require.EqualError(t, err, "[variable:1232]Incorrect argument type to variable 'tidb_top_sql_max_meta_count'")
	tk.MustExec("set @@global.tidb_top_sql_max_meta_count='-1';")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_top_sql_max_meta_count value: '-1'"))
	tk.MustQuery("select @@global.tidb_top_sql_max_meta_count;").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_top_sql_max_meta_count='10001';")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_top_sql_max_meta_count value: '10001'"))
	tk.MustQuery("select @@global.tidb_top_sql_max_meta_count;").Check(testkit.Rows("10000"))

	tk.MustExec("set @@global.tidb_top_sql_max_meta_count=5000;")
	tk.MustQuery("select @@global.tidb_top_sql_max_meta_count;").Check(testkit.Rows("5000"))
	require.Equal(t, int64(5000), topsqlstate.GlobalState.MaxCollect.Load())

	tk.MustQuery("show variables like '%top_sql%'").Check(testkit.Rows("tidb_enable_top_sql OFF", "tidb_top_sql_max_meta_count 5000", "tidb_top_sql_max_time_series_count 20"))
	tk.MustQuery("show global variables like '%top_sql%'").Check(testkit.Rows("tidb_enable_top_sql OFF", "tidb_top_sql_max_meta_count 5000", "tidb_top_sql_max_time_series_count 20"))
}

func TestDivPrecisionIncrement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Default is 4.
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("4"))

	tk.MustExec("set @@div_precision_increment = 4")
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("4"))
	// Min val is 0.
	tk.MustExec("set @@div_precision_increment = -1")
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("0"))

	tk.MustExec("set @@div_precision_increment = 0")
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("0"))

	tk.MustExec("set @@div_precision_increment = 30")
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("30"))

	tk.MustExec("set @@div_precision_increment = 8")
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("8"))

	// Max val is 30.
	tk.MustExec("set @@div_precision_increment = 31")
	tk.MustQuery("select @@div_precision_increment;").Check(testkit.Rows("30"))

	// Test set global.
	tk.MustExec("set global div_precision_increment = 4")
}
