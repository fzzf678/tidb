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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/stretchr/testify/require"
)

// utilCleanBindingEnv cleans the binding environment.
func utilCleanBindingEnv(tk *testkit.TestKit) {
	tk.MustExec("update mysql.bind_info set status='deleted' where source != 'builtin'")
	tk.MustExec(`admin reload bindings`)
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	tk.MustExec(`admin reload bindings`)
}

func TestPrepareCacheWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t2(a int, b int, c int, key idx_b(b), key idx_c(c))")

	// TestDMLSQLBind
	tk.MustExec("prepare stmt1 from 'delete from t1 where b = 1 and c > 1';")
	tk.MustExec("execute stmt1;")
	require.Equal(t, "t1:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_b(b)")
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1,idx_c) */ from t1 where b = 1 and c > 1")

	tk.MustExec("execute stmt1;")
	require.Equal(t, "t1:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_c(c)")

	tk.MustExec("prepare stmt2 from 'delete t1, t2 from t1 inner join t2 on t1.b = t2.b';")
	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "HashJoin")
	tk.MustExec("execute stmt2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b using delete /*+ inl_join(t1) */ t1, t2 from t1 inner join t2 on t1.b = t2.b")

	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexJoin")

	tk.MustExec("prepare stmt3 from 'update t1 set a = 1 where b = 1 and c > 1';")
	tk.MustExec("execute stmt3;")
	require.Equal(t, "t1:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_b(b)")
	tk.MustExec("execute stmt3;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1,idx_c) */ t1 set a = 1 where b = 1 and c > 1")

	tk.MustExec("execute stmt3;")
	require.Equal(t, "t1:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_c(c)")

	tk.MustExec("prepare stmt4 from 'update t1, t2 set t1.a = 1 where t1.b = t2.b';")
	tk.MustExec("execute stmt4;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "HashJoin")
	tk.MustExec("execute stmt4;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")

	tk.MustExec("execute stmt4;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexJoin")

	tk.MustExec("prepare stmt5 from 'insert into t1 select * from t2 where t2.b = 2 and t2.c > 2';")
	tk.MustExec("execute stmt5;")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_b(b)")
	tk.MustExec("execute stmt5;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert /*+ use_index(t2,idx_c) */ into t1 select * from t2 where t2.b = 1 and t2.c > 1")

	tk.MustExec("execute stmt5;")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_b(b)")

	tk.MustExec("drop global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")

	tk.MustExec("execute stmt5;")
	require.Equal(t, "t2:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_c(c)")

	tk.MustExec("prepare stmt6 from 'replace into t1 select * from t2 where t2.b = 2 and t2.c > 2';")
	tk.MustExec("execute stmt6;")
	require.Equal(t, "t2:idx_b", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_b(b)")
	tk.MustExec("execute stmt6;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")

	tk.MustExec("execute stmt6;")
	require.Equal(t, "t2:idx_c", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "idx_c(c)")

	// TestExplain
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")

	tk.MustExec("prepare stmt1 from 'SELECT * from t1,t2 where t1.id = t2.id';")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "HashJoin")
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt2 from 'SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id';")
	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "MergeJoin")
	tk.MustExec("execute stmt2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "MergeJoin")

	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")

	tk.MustExec("create index index_id on t1(id)")
	tk.MustExec("prepare stmt1 from 'SELECT * from t1 use index(index_id)';")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexReader")
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for SELECT * from t1 using SELECT * from t1 ignore index(index_id)")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustNotHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexReader")
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// Add test for SetOprStmt
	tk.MustExec("prepare stmt1 from 'SELECT * from t1 union SELECT * from t1';")
	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexReader")
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt2 from 'SELECT * from t1 use index(index_id) union SELECT * from t1';")
	tk.MustExec("execute stmt2;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexReader")
	tk.MustExec("execute stmt2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("create global binding for SELECT * from t1 union SELECT * from t1 using SELECT * from t1 use index(index_id) union SELECT * from t1")

	tk.MustExec("execute stmt1;")
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustHavePlan("for connection "+strconv.FormatUint(tkProcess.ID, 10), "IndexReader")

	tk.MustExec("drop global binding for SELECT * from t1 union SELECT * from t1")

	// TestBindingSymbolList
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")
	tk.MustExec("prepare stmt1 from 'select a, b from t where a = 3 limit 1, 100';")
	tk.MustExec("execute stmt1;")
	require.Equal(t, "t:ia", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "ia(a)")
	tk.MustExec("execute stmt1;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`create global binding for select a, b from t where a = 1 limit 0, 1 using select a, b from t use index (ib) where a = 1 limit 0, 1`)

	// after binding
	tk.MustExec("execute stmt1;")
	require.Equal(t, "t:ib", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tkProcess = tk.Session().ShowProcess()
	ps = []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustUseIndexForConnection(strconv.FormatUint(tkProcess.ID, 10), "ib(b)")
}

func TestIssue50646(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database TICASE;`)
	tk.MustExec(`use TICASE;`)
	tk.MustExec(`create table t(a int, b int, index idx(a));`)
	tk.MustExec(`create table t1(a int, b int, index idx(a));`)
	tk.MustExec(`create global binding for delete t, t1 from t use index(idx) join t1 use index(idx) on t.a=t1.a using delete /*+ merge_join(t) */ t, t1 from t use index(idx) join t1 use index(idx) on t.a=t1.a;`)
	tk.MustHavePlan(`delete /*+ inl_merge_join(t) */ t, t1 from t ignore index(idx) join t1 ignore index(idx) on t.a=t1.a;`, "MergeJoin")
	tk.MustExec(`delete t, t1 from t ignore index(idx) join t1 ignore index(idx) on t.a=t1.a;`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
}

func TestStmtHints(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select /*+ MAX_EXECUTION_TIME(100), SET_VAR(TIKV_CLIENT_READ_TIMEOUT=20), MEMORY_QUOTA(2 GB) */ * from t use index(idx)")
	tk.MustQuery("select * from t")
	require.Equal(t, int64(2147483648), tk.Session().GetSessionVars().MemTracker.GetBytesLimit())
	require.Equal(t, uint64(100), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)
	require.Equal(t, uint64(20), tk.Session().GetSessionVars().GetTiKVClientReadTimeout())
	tk.MustQuery("select a, b from t")
	require.Equal(t, int64(1073741824), tk.Session().GetSessionVars().MemTracker.GetBytesLimit())
	require.Equal(t, uint64(0), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)
	// TODO(crazycs520): Fix me.
	//require.Equal(t, uint64(0), tk.Session().GetSessionVars().GetTiKVClientReadTimeout())
}

func TestBindingWithIsolationRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustExec("analyze table t")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
		Count:     1,
		Available: true,
	}
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	require.Equal(t, "cop[tikv]", rows[len(rows)-1][2])
	// Even if we build a binding use index for SQL, but after we set the isolation read for TiFlash, it choose TiFlash instead of index of TiKV.
	tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	require.Equal(t, "mpp[tiflash]", rows[len(rows)-1][2])
}

func TestInvisibleIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, unique idx_a(a), index idx_b(b) invisible)")
	tk.MustContainErrMsg("create global binding for select * from t using select * from t use index(idx_b)",
		"[planner:1176]Key 'idx_b' doesn't exist in table 't'")

	// Create bind using index
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a)")

	tk.MustQuery("select * from t")
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])
	tk.MustUseIndex("select * from t", "idx_a(a)")

	tk.MustExec(`prepare stmt1 from 'select * from t'`)
	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 1)
	require.Equal(t, "t:idx_a", tk.Session().GetSessionVars().StmtCtx.IndexNames[0])

	// And then make this index invisible
	tk.MustExec("alter table t alter index idx_a invisible")
	tk.MustQuery("select * from t")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("execute stmt1")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.IndexNames, 0)

	tk.MustExec("drop global binding for select * from t")
}

func TestGCBindRecord(t *testing.T) {
	// set lease for gc tests
	originLease := bindinfo.Lease
	bindinfo.Lease = 0
	defer func() {
		bindinfo.Lease = originLease
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")

	tk.MustExec("create global binding for select * from t where a = 1 using select * from t use index(a) where a = 1")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` = ?", rows[0][0])
	require.Equal(t, bindinfo.StatusEnabled, rows[0][3])
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		bindinfo.StatusEnabled,
	))

	h := dom.BindingHandle()
	// bindinfo.Lease is set to 0 for test env in SetUpSuite.
	require.NoError(t, h.GCBinding())
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "select * from `test` . `t` where `a` = ?", rows[0][0])
	require.Equal(t, bindinfo.StatusEnabled, rows[0][3])
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		bindinfo.StatusEnabled,
	))

	tk.MustExec("drop global binding for select * from t where a = 1")
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		"deleted",
	))
	require.NoError(t, h.GCBinding())
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows())
}

func TestBindSQLDigest(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a int, b int, key(a), key(b))")

	cases := []struct {
		origin string
		hint   string
	}{
		// agg hints
		{"select count(1) from t", "select /*+ hash_agg() */ count(1) from t"},
		{"select count(1) from t", "select /*+ stream_agg() */ count(1) from t"},
		// join hints
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ merge_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_smj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_hj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		// index hints
		{"select * from t", "select * from t use index(primary)"},
		{"select * from t", "select /*+ use_index(primary) */ * from t"},
		{"select * from t", "select * from t use index(a)"},
		{"select * from t", "select /*+ use_index(a) */ * from t use index(a)"},
		{"select * from t", "select * from t use index(b)"},
		{"select * from t", "select /*+ use_index(b) */ * from t use index(b)"},
		{"select a, b from t where a=1 or b=1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a=1 or b=1"},
		{"select * from t where a=1", "select /*+ ignore_index(t, a) */ * from t where a=1"},
		// push-down hints
		{"select * from t limit 10", "select /*+ limit_to_cop() */ * from t limit 10"},
		{"select a, count(*) from t group by a", "select /*+ agg_to_cop() */ a, count(*) from t group by a"},
		// index-merge hints
		{"select a, b from t where a>1 or b>1", "select /*+ no_index_merge() */ a, b from t where a>1 or b>1"},
		{"select a, b from t where a>1 or b>1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a>1 or b>1"},
		// runtime hints
		{"select * from t", "select /*+ memory_quota(1024 MB) */ * from t"},
		{"select * from t", "select /*+ max_execution_time(1000) */ * from t"},
		{"select * from t", "select /*+ set_var(tikv_client_read_timeout=1000) */ * from t"},
		// storage hints
		{"select * from t", "select /*+ read_from_storage(tikv[t]) */ * from t"},
		// others
		{"select t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)", "select /*+ use_toja(true) */ t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)"},
	}
	for _, c := range cases {
		stmtsummary.StmtSummaryByDigestMap.Clear()
		utilCleanBindingEnv(tk)
		sql := "create global binding for " + c.origin + " using " + c.hint
		tk.MustExec(sql)
		res := tk.MustQuery(`show global bindings`).Rows()
		require.Equal(t, len(res[0]), 11)

		parser4binding := parser.New()
		originNode, err := parser4binding.ParseOneStmt(c.origin, "utf8mb4", "utf8mb4_general_ci")
		require.NoError(t, err)
		_, sqlDigestWithDB := parser.NormalizeDigestForBinding(bindinfo.RestoreDBForBinding(originNode, "test"))
		require.Equal(t, res[0][9], sqlDigestWithDB.String())
	}
}

func TestSimplifiedCreateBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)

	check := func(scope, sql, binding string) {
		r := tk.MustQuery(fmt.Sprintf("show %s bindings", scope)).Rows()
		require.Equal(t, len(r), 1)
		require.Equal(t, r[0][0].(string), sql)
		require.Equal(t, r[0][1].(string), binding)
	}

	tk.MustExec(`create binding using select /*+ use_index(t, a) */ * from t`)
	check("", "select * from `test` . `t`", "SELECT /*+ use_index(`t` `a`)*/ * FROM `test`.`t`")
	tk.MustExec(`drop binding for select * from t`)
	tk.MustExec(`create binding using select /*+ use_index(t, a) */ * from t where a<10`)
	check("", "select * from `test` . `t` where `a` < ?", "SELECT /*+ use_index(`t` `a`)*/ * FROM `test`.`t` WHERE `a` < 10")
	tk.MustExec(`drop binding for select * from t where a<10`)
	tk.MustExec(`create global binding using select /*+ use_index(t, a) */ * from t where a in (1)`)
	check("global", "select * from `test` . `t` where `a` in ( ... )", "SELECT /*+ use_index(`t` `a`)*/ * FROM `test`.`t` WHERE `a` IN (1)")
	tk.MustExec(`drop global binding for select * from t where a in (1)`)
	tk.MustExec(`create global binding using select /*+ use_index(t, a) */ * from t where a in (1,2,3)`)
	check("global", "select * from `test` . `t` where `a` in ( ... )", "SELECT /*+ use_index(`t` `a`)*/ * FROM `test`.`t` WHERE `a` IN (1,2,3)")
	tk.MustExec(`drop global binding for select * from t where a in (1,2,3)`)
}

func TestDropBindBySQLDigest(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a int, b int, key(a), key(b))")

	cases := []struct {
		origin string
		hint   string
	}{
		// agg hints
		{"select count(1) from t", "select /*+ hash_agg() */ count(1) from t"},
		{"select count(1) from t", "select /*+ stream_agg() */ count(1) from t"},
		// join hints
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ merge_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_smj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_hj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ tidb_inlj(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		{"select * from t t1, t t2 where t1.a=t2.a", "select /*+ inl_hash_join(t1, t2) */ * from t t1, t t2 where t1.a=t2.a"},
		// index hints
		{"select * from t", "select * from t use index(primary)"},
		{"select * from t", "select /*+ use_index(primary) */ * from t"},
		{"select * from t", "select * from t use index(a)"},
		{"select * from t", "select /*+ use_index(a) */ * from t use index(a)"},
		{"select * from t", "select * from t use index(b)"},
		{"select * from t", "select /*+ use_index(b) */ * from t use index(b)"},
		{"select a, b from t where a=1 or b=1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a=1 or b=1"},
		{"select * from t where a=1", "select /*+ ignore_index(t, a) */ * from t where a=1"},
		// push-down hints
		{"select * from t limit 10", "select /*+ limit_to_cop() */ * from t limit 10"},
		{"select a, count(*) from t group by a", "select /*+ agg_to_cop() */ a, count(*) from t group by a"},
		// index-merge hints
		{"select a, b from t where a>1 or b>1", "select /*+ no_index_merge() */ a, b from t where a>1 or b>1"},
		{"select a, b from t where a>1 or b>1", "select /*+ use_index_merge(t, a, b) */ a, b from t where a>1 or b>1"},
		// runtime hints
		{"select * from t", "select /*+ memory_quota(1024 MB) */ * from t"},
		{"select * from t", "select /*+ max_execution_time(1000) */ * from t"},
		{"select * from t", "select /*+ set_var(tikv_client_read_timeout=1000) */ * from t"},
		// storage hints
		{"select * from t", "select /*+ read_from_storage(tikv[t]) */ * from t"},
		// others
		{"select t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)", "select /*+ use_toja(true) */ t1.a, t1.b from t t1 where t1.a in (select t2.a from t t2)"},
	}

	h := dom.BindingHandle()
	// global scope
	for _, c := range cases {
		utilCleanBindingEnv(tk)
		sql := "create global binding for " + c.origin + " using " + c.hint
		tk.MustExec(sql)
		h.LoadFromStorageToCache(true)
		res := tk.MustQuery(`show global bindings`).Rows()

		require.Equal(t, len(res), 1)
		require.Equal(t, len(res[0]), 11)
		drop := fmt.Sprintf("drop global binding for sql digest '%s'", res[0][9])
		tk.MustExec(drop)
		require.NoError(t, h.GCBinding())
		h.LoadFromStorageToCache(true)
		tk.MustQuery("show global bindings").Check(testkit.Rows())
	}

	// session scope
	for _, c := range cases {
		utilCleanBindingEnv(tk)
		sql := "create binding for " + c.origin + " using " + c.hint
		tk.MustExec(sql)
		res := tk.MustQuery(`show bindings`).Rows()

		require.Equal(t, len(res), 1)
		require.Equal(t, len(res[0]), 11)
		drop := fmt.Sprintf("drop binding for sql digest '%s'", res[0][9])
		tk.MustExec(drop)
		require.NoError(t, h.GCBinding())
		tk.MustQuery("show bindings").Check(testkit.Rows())
	}

	// exception cases
	tk.MustGetErrMsg(fmt.Sprintf("drop binding for sql digest '%s'", ""), "sql digest is empty")
}

func TestJoinOrderHintWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2, t3;")
	tk.MustExec("create table t(a int, b int, key(a));")
	tk.MustExec("create table t1(a int, b int, key(a));")
	tk.MustExec("create table t2(a int, b int, key(a));")
	tk.MustExec("create table t3(a int, b int, key(a));")

	tk.MustExec("create global binding for select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b using select /*+ leading(t2) */ * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustExec("select * from t1 join t2 on t1.a=t2.a left join t3 on t2.b=t3.b")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from ( `test` . `t1` join `test` . `t2` on `t1` . `a` = `t2` . `a` ) left join `test` . `t3` on `t2` . `b` = `t3` . `b`")

	tk.MustExec("drop global binding for select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t3.b")
}

func showBinding(tk *testkit.TestKit, showStmt string) [][]any {
	rows := tk.MustQuery(showStmt).Sort().Rows()
	result := make([][]any, len(rows))
	for i, r := range rows {
		result[i] = append(result[i], r[:4]...)
		result[i] = append(result[i], r[8:10]...)
	}
	return result
}

func removeAllBindings(tk *testkit.TestKit, global bool) {
	scope := "session"
	if global {
		scope = "global"
	}
	res := showBinding(tk, fmt.Sprintf("show %v bindings", scope))
	digests := make([]string, 0, len(res))
	for _, r := range res {
		if r[4] == "builtin" {
			continue
		}
		digests = append(digests, r[5].(string))
	}
	if len(digests) == 0 {
		return
	}
	// test DROP BINDING FOR SQL DIGEST can handle empty strings correctly
	digests = append(digests, "", "", "")
	// randomly split digests into 4 groups using random number
	// shuffle the slice
	rand.Shuffle(len(digests), func(i, j int) {
		digests[i], digests[j] = digests[j], digests[i]
	})
	split := make([][]string, 4)
	for i, d := range digests {
		split[i%4] = append(split[i%4], d)
	}
	// group 0: wrap with ' then connect by ,
	var g0 string
	for _, d := range split[0] {
		g0 += "'" + d + "',"
	}
	// group 1: connect by , and set into a user variable
	tk.MustExec(fmt.Sprintf("set @a = '%v'", strings.Join(split[1], ",")))
	g1 := "@a,"
	var g2 string
	for _, d := range split[2] {
		g2 += "'" + d + "',"
	}
	// group 2: connect by , and put into a normal string
	g3 := "'" + strings.Join(split[3], ",") + "'"
	tk.MustExec(fmt.Sprintf("drop %v binding for sql digest %s %s %s %s", scope, g0, g1, g2, g3))
	tk.MustQuery(fmt.Sprintf("show %v bindings", scope)).Check(testkit.Rows()) // empty
}

func testFuzzyBindingHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for _, db := range []string{"db1", "db2", "db3"} {
		tk.MustExec(`create database ` + db)
		tk.MustExec(`use ` + db)
		tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t3 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	}
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)

	for _, c := range []struct {
		binding   string
		qTemplate string
	}{
		// use index
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1 where a=1`,
			`select * from %st1 where a=1000`},
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1 where d<1`,
			`select * from %st1 where d<10000`},
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from %st1, t2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from t1, %st2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from %st1, t2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from t1, %st2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2, *.t3 where t1.d<1`,
			`select * from %st1, t2, t3 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2, *.t3 where t1.d<1`,
			`select * from t1, t2, %st3 where t1.d<100`},

		// ignore index
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b=1`,
			`select * from %st1 where b=1000`},
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b>1`,
			`select * from %st1 where b>1000`},
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b in (1,2)`,
			`select * from %st1 where b in (1)`},
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b in (1,2)`,
			`select * from %st1 where b in (1,2,3,4,5)`},

		// order index hint
		{`create global binding using select /*+ order_index(t1, a) */ a from *.t1 where a<10 order by a limit 10`,
			`select a from %st1 where a<10000 order by a limit 10`},
		{`create global binding using select /*+ order_index(t1, b) */ b from *.t1 where b>10 order by b limit 1111`,
			`select b from %st1 where b>2 order by b limit 10`},

		// no order index hint
		{`create global binding using select /*+ no_order_index(t1, c) */ c from *.t1 where c<10 order by c limit 10`,
			`select c from %st1 where c<10000 order by c limit 10`},
		{`create global binding using select /*+ no_order_index(t1, d) */ d from *.t1 where d>10 order by d limit 1111`,
			`select d from %st1 where d>2 order by d limit 10`},

		// agg hint
		{`create global binding using select /*+ hash_agg() */ count(*) from *.t1 group by a`,
			`select count(*) from %st1 group by a`},
		{`create global binding using select /*+ stream_agg() */ count(*) from *.t1 group by b`,
			`select count(*) from %st1 group by b`},

		// to_cop hint
		{`create global binding using select /*+ agg_to_cop() */ sum(a) from *.t1`,
			`select sum(a) from %st1`},
		{`create global binding using select /*+ limit_to_cop() */ a from *.t1 limit 10`,
			`select a from %st1 limit 101`},

		// index merge hint
		{`create global binding using select /*+ use_index_merge(t1, c, d) */ * from *.t1 where c=1 or d=1`,
			`select * from %st1 where c=1000 or d=1000`},
		{`create global binding using select /*+ no_index_merge() */ * from *.t1 where a=1 or b=1`,
			`select * from %st1 where a=1000 or b=1000`},

		// join type hint
		{`create global binding using select /*+ hash_join(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create global binding using select /*+ hash_join(t2) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ hash_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ hash_join_build(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ hash_join_probe(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ merge_join(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create global binding using select /*+ merge_join(t2) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ merge_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ inl_join(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create global binding using select /*+ inl_join(t2) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ inl_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},

		// no join type hint
		{`create global binding using select /*+ no_hash_join(t1) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ no_hash_join(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ no_hash_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ no_merge_join(t1) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ no_merge_join(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ no_merge_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ no_index_join(t1) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ no_index_join(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ no_index_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},

		// join order hint
		{`create global binding using select /*+ leading(t2) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ leading(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ leading(t2, t1) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ leading(t1, t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ leading(t1) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ leading(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ leading(t2,t3) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ leading(t2,t3,t1) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
	} {
		removeAllBindings(tk, true)
		tk.MustExec(c.binding)
		for _, currentDB := range []string{"db1", "db2", "db3"} {
			tk.MustExec(`use ` + currentDB)
			for _, db := range []string{"db1.", "db2.", "db3.", ""} {
				query := fmt.Sprintf(c.qTemplate, db)
				tk.MustExec(query)
				tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
				tk.MustExec(query)
				tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
			}
		}
	}
}

func TestFuzzyBindingHints(t *testing.T) {
	t.Skip("fix later on")
	testFuzzyBindingHints(t)
}

func TestBatchDropBindings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk.MustExec(`create table t3 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk.MustExec(`create global binding for select * from t1 using select /*+ use_index(t1, a) */ * from t1`)
	tk.MustExec(`create global binding for select * from t1 where b < 1 using select /*+ use_index(t1,b) */ * from t1 where b < 1`)
	tk.MustExec(`create global binding for select * from t1 where c < 1 using select /*+ use_index(t1,c) */ * from t1 where c < 1`)
	tk.MustExec(`create global binding for select * from t1 join t2 on t1.a = t2.a using select /*+ hash_join(t1) */ * from t1 join t2 on t1.a = t2.a`)
	tk.MustExec(`create global binding for select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b where t1.a = 1 using select /*+ leading(t3,t2,t1) */ * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b where t1.a = 1`)
	tk.MustExec(`create global binding for select * from t1 where a in (select sum(b) from t2) using select /*+ agg_to_cop(@sel_2) */ * from t1 where a in (select sum(b) from t2)`)
	tk.MustExec(`create global binding for select * from t2 where a = 1 and b = 2 and c = 3 using select * from t2 ignore index (b) where a = 1 and b = 2 and c = 3`)
	tk.MustExec(`create global binding for select * from t2 where a = 1 and b = 2 and c = 3 using select * from t2 use index (b) where a = 1 and b = 2 and c = 3`)

	tk.MustExec(`create session binding for select * from t1 using select /*+ use_index(t1, a) */ * from t1`)
	tk.MustExec(`create session binding for select * from t1 where b < 1 using select /*+ use_index(t1, b) */ * from t1 where b < 1`)
	tk.MustExec(`create session binding for select * from t1 where c < 1 using select /*+ use_index(t1, c) */ * from t1 where c < 1`)
	tk.MustExec(`create session binding for select * from t1 join t2 on t1.a = t2.a using select /*+ hash_join( t1) */ * from t1 join t2 on t1.a = t2.a`)
	tk.MustExec(`create session binding for select * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b where t1. a = 1 using select /*+ leading(t3,t2,t1) */ * from t1 join t2 on t1.a = t2.a join t3 on t2.b = t3.b where t1.a = 1`)
	tk.MustExec(`create session binding for select * from t1 where a in (select sum( b) from t2) using select /*+ agg_to_cop(@sel_2) */ * from t1 where a in (select sum(b) from t2)`)
	tk.MustExec(`create session binding for select * from t2 where a = 1 and b = 2 and c = 3 using select * from t2 ignore index (b) where a = 1 and b = 2 and c = 3`)
	tk.MustExec(`create session binding for select * from t2 where a = 1 and b = 2 and c = 3 using select * from t2 use index (b) where a = 1 and b = 2 and c = 3`)
	removeAllBindings(tk, true)
	removeAllBindings(tk, false)
}

func TestInvalidBindingCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int)`)

	cases := []struct {
		SQL string
		Err string
	}{
		{"select * from t where c=1", "[planner:1054]Unknown column 'c' in 'where clause'"},
		{"select * from t where a=1 and c=1", "[planner:1054]Unknown column 'c' in 'where clause'"},
		{"select * from dbx.t", "[schema:1146]Table 'dbx.t' doesn't exist"},
		{"select * from t1", "[schema:1146]Table 'test.t1' doesn't exist"},
		{"select * from t1, t", "[schema:1146]Table 'test.t1' doesn't exist"},
		{"select * from t use index(c)", "[planner:1176]Key 'c' doesn't exist in table 't'"},
	}

	for _, c := range cases {
		for _, scope := range []string{"session", "global"} {
			sql := fmt.Sprintf("create %v binding using %v", scope, c.SQL)
			tk.MustGetErrMsg(sql, c.Err)
		}
	}

	// cross-db bindings or bindings with parameters can bypass the check, which is expected.
	// We'll optimize this check further in the future.
	tk.MustExec("create binding using select * from *.t where c=1")
	tk.MustExec("create binding using select * from t where c=?")
}
