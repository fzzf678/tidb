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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapclient_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/mock"
	importclient "github.com/pingcap/tidb/br/pkg/restore/internal/import_client"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var mc *mock.Cluster

func TestCreateTables(t *testing.T) {
	m := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(m.PDClient, m.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, m.Storage)
	require.NoError(t, err)

	info, err := m.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(ast.NewCIStr("test"))
	require.True(t, isExist)

	client.SetBatchDdlSize(1)
	tables := make([]*metautil.Table, 4)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	for i := len(tables) - 1; i >= 0; i-- {
		tables[i] = &metautil.Table{
			DB: dbSchema,
			Info: &model.TableInfo{
				ID:   int64(i),
				Name: ast.NewCIStr("test" + strconv.Itoa(i)),
				Columns: []*model.ColumnInfo{{
					ID:        1,
					Name:      ast.NewCIStr("id"),
					FieldType: *intField,
					State:     model.StatePublic,
				}},
				Charset: "utf8mb4",
				Collate: "utf8mb4_bin",
			},
		}
	}
	rules, newTables, err := client.CreateTablesTest(m.Domain, tables, 0)
	require.NoError(t, err)
	// make sure tables and newTables have same order
	for i, tbl := range tables {
		require.Equal(t, tbl.Info.Name, newTables[i].Name)
	}
	for _, nt := range newTables {
		require.Regexp(t, "test[0-3]", nt.Name.String())
	}
	oldTableIDExist := make(map[int64]bool)
	newTableIDExist := make(map[int64]bool)
	for _, tr := range rules.Data {
		oldTableID := tablecodec.DecodeTableID(tr.GetOldKeyPrefix())
		require.False(t, oldTableIDExist[oldTableID], "table rule duplicate old table id")
		oldTableIDExist[oldTableID] = true

		newTableID := tablecodec.DecodeTableID(tr.GetNewKeyPrefix())
		require.False(t, newTableIDExist[newTableID], "table rule duplicate new table id")
		newTableIDExist[newTableID] = true
	}

	for i := range tables {
		require.True(t, oldTableIDExist[int64(i)], "table rule does not exist")
	}
}

func getStartedMockedCluster(t *testing.T) *mock.Cluster {
	t.Helper()
	cluster, err := mock.NewCluster()
	require.NoError(t, err)
	err = cluster.Start()
	require.NoError(t, err)
	return cluster
}

func TestNeedCheckTargetClusterFresh(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	// not set filter and first run with checkpoint
	require.True(t, client.NeedCheckFreshCluster(false, false))

	// skip check when has checkpoint
	require.False(t, client.NeedCheckFreshCluster(false, true))

	// skip check when set --filter
	require.False(t, client.NeedCheckFreshCluster(true, true))

	// skip check when has set --filter and has checkpoint
	require.False(t, client.NeedCheckFreshCluster(true, false))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data", "return(false)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data"))
	}()
	// skip check when increment backup
	require.False(t, client.NeedCheckFreshCluster(false, false))
}

func TestCheckTargetClusterFresh(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, client.EnsureNoUserTables())

	require.NoError(t, client.CreateDatabases(ctx, []*metautil.Database{{Info: &model.DBInfo{Name: ast.NewCIStr("user_db")}}}))
	require.True(t, berrors.ErrRestoreNotFreshCluster.Equal(client.EnsureNoUserTables()))
}

func TestCheckTargetClusterFreshWithTable(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	info, err := cluster.Domain.GetSnapshotInfoSchema(math.MaxUint64)
	require.NoError(t, err)
	dbSchema, isExist := info.SchemaByName(ast.NewCIStr("test"))
	require.True(t, isExist)
	intField := types.NewFieldType(mysql.TypeLong)
	intField.SetCharset("binary")
	table := &metautil.Table{
		DB: dbSchema,
		Info: &model.TableInfo{
			ID:   int64(1),
			Name: ast.NewCIStr("t"),
			Columns: []*model.ColumnInfo{{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *intField,
				State:     model.StatePublic,
			}},
			Charset: "utf8mb4",
			Collate: "utf8mb4_bin",
		},
	}
	_, _, err = client.CreateTablesTest(cluster.Domain, []*metautil.Table{table}, 0)
	require.NoError(t, err)

	require.True(t, berrors.ErrRestoreNotFreshCluster.Equal(client.EnsureNoUserTables()))
}

func TestInitFullClusterRestore(t *testing.T) {
	cluster := mc
	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	// explicit filter
	client.InitFullClusterRestore(true, true, true)
	require.False(t, client.IsFullClusterRestore())

	client.InitFullClusterRestore(false, true, true)
	require.True(t, client.IsFullClusterRestore())
	// set it to false again
	client.InitFullClusterRestore(false, true, false)
	require.False(t, client.IsFullClusterRestore())

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/restore/snap_client/mock-incr-backup-data"))
	}()
	client.InitFullClusterRestore(false, true, true)
	require.False(t, client.IsFullClusterRestore())
}

// Mock ImporterClient interface
type FakeImporterClient struct {
	importclient.ImporterClient
}

// Record the stores that have communicated
type RecordStores struct {
	mu     sync.Mutex
	stores []uint64
}

func NewRecordStores() RecordStores {
	return RecordStores{stores: make([]uint64, 0)}
}

func (r *RecordStores) put(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stores = append(r.stores, id)
}

func (r *RecordStores) sort() {
	r.mu.Lock()
	defer r.mu.Unlock()
	slices.Sort(r.stores)
}

func (r *RecordStores) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.stores)
}

func (r *RecordStores) get(i int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stores[i]
}

func (r *RecordStores) toString() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprintf("%v", r.stores)
}

var recordStores RecordStores

const (
	SET_SPEED_LIMIT_ERROR = 999999
	WORKING_TIME          = 100
)

func (fakeImportCli FakeImporterClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	if storeID == SET_SPEED_LIMIT_ERROR {
		return nil, fmt.Errorf("storeID:%v ERROR", storeID)
	}

	time.Sleep(WORKING_TIME * time.Millisecond) // simulate doing 100 ms work
	recordStores.put(storeID)
	return nil, nil
}

func (fakeImportCli FakeImporterClient) CheckMultiIngestSupport(ctx context.Context, stores []uint64) error {
	return nil
}

func TestSetSpeedLimit(t *testing.T) {
	mockStores := []*metapb.Store{
		{Id: 1},
		{Id: 2},
		{Id: 3},
		{Id: 4},
		{Id: 5},
		{Id: 6},
		{Id: 7},
		{Id: 8},
		{Id: 9},
		{Id: 10},
	}

	// 1. The cost of concurrent communication is expected to be less than the cost of serial communication.
	client := snapclient.NewRestoreClient(
		split.NewFakePDClient(mockStores, false, nil), nil, nil, split.DefaultTestKeepaliveCfg)
	ctx := context.Background()

	recordStores = NewRecordStores()
	start := time.Now()
	err := snapclient.MockCallSetSpeedLimit(ctx, mockStores, FakeImporterClient{}, client, 10)
	cost := time.Since(start)
	require.NoError(t, err)

	recordStores.sort()
	t.Logf("Total Cost: %v\n", cost)
	t.Logf("Has Communicated: %v\n", recordStores.toString())

	serialCost := len(mockStores) * WORKING_TIME
	require.Less(t, cost, time.Duration(serialCost)*time.Millisecond)
	require.Equal(t, len(mockStores), recordStores.len())
	for i := range recordStores.len() {
		require.Equal(t, mockStores[i].Id, recordStores.get(i))
	}

	// 2. Expect the number of communicated stores to be less than the length of the mockStore
	// Because subsequent unstarted communications are aborted when an error is encountered.
	recordStores = NewRecordStores()
	mockStores[5].Id = SET_SPEED_LIMIT_ERROR // setting a fault store
	client = snapclient.NewRestoreClient(
		split.NewFakePDClient(mockStores, false, nil), nil, nil, split.DefaultTestKeepaliveCfg)

	// Concurrency needs to be less than the number of stores
	err = snapclient.MockCallSetSpeedLimit(ctx, mockStores, FakeImporterClient{}, client, 2)
	require.Error(t, err)
	t.Log(err)

	recordStores.sort()
	sort.Slice(mockStores, func(i, j int) bool { return mockStores[i].Id < mockStores[j].Id })
	t.Logf("Has Communicated: %v\n", recordStores.toString())
	require.Less(t, recordStores.len(), len(mockStores))
	for i := range recordStores.len() {
		require.Equal(t, mockStores[i].Id, recordStores.get(i))
	}
}

func TestSortTablesBySchemaID(t *testing.T) {
	// Create test tables with different schema IDs in mixed order
	tables := []*metautil.Table{
		createTestTable(2, 3),
		createTestTable(1, 2),
		createTestTable(3, 5),
		createTestTable(1, 1),
		createTestTable(2, 4),
		createTestTable(3, 6),
		createTestTable(6, 7),
	}

	sorted := snapclient.SortTablesBySchemaID(tables)

	require.Len(t, sorted, 7, "Should have 7 tables after sorting")

	expectedSchemaIDs := []int64{1, 1, 2, 2, 3, 3, 6}
	expectedTableIDs := []int64{1, 2, 3, 4, 5, 6, 7}
	actualSchemaIDs := make([]int64, 7)
	actualTableIDs := make([]int64, 7)
	for i, table := range sorted {
		actualSchemaIDs[i] = table.DB.ID
		actualTableIDs[i] = table.Info.ID
	}

	require.Equal(t, expectedSchemaIDs, actualSchemaIDs, "Tables should be sorted by schema ID")
	require.Equal(t, expectedTableIDs, actualTableIDs, "Tables should be sorted by table ID")
}

// Helper function to create a test table with given IDs
func createTestTable(schemaID, tableID int64) *metautil.Table {
	dbInfo := &model.DBInfo{
		ID: schemaID,
	}

	tableInfo := &model.TableInfo{
		ID: tableID,
	}

	return &metautil.Table{
		DB:   dbInfo,
		Info: tableInfo,
	}
}

func generateMetautilTable(dbName string, tableID int64, partitionIDs ...int64) *metautil.Table {
	return generateMetautilTableWithName(dbName, "", tableID, partitionIDs...)
}

func generateMetautilTableWithName(dbName string, tableName string, tableID int64, partitionIDs ...int64) *metautil.Table {
	var partition *model.PartitionInfo
	if len(partitionIDs) > 0 {
		partition = &model.PartitionInfo{
			Definitions: make([]model.PartitionDefinition, 0, len(partitionIDs)),
		}
		for _, partitionID := range partitionIDs {
			partition.Definitions = append(partition.Definitions, model.PartitionDefinition{
				ID: partitionID,
			})
		}
	}
	return &metautil.Table{
		DB: &model.DBInfo{
			Name: ast.NewCIStr(dbName),
		},
		Info: &model.TableInfo{
			ID:        tableID,
			Name:      ast.NewCIStr(tableName),
			Partition: partition,
		},
	}
}

func TestAllocTableIDs(t *testing.T) {
	// cannot use shared `mc`, other parallel case may change it.
	cluster := getStartedMockedCluster(t)
	defer cluster.Stop()

	g := gluetidb.New()
	client := snapclient.NewRestoreClient(cluster.PDClient, cluster.PDHTTPCli, nil, split.DefaultTestKeepaliveCfg)
	err := client.InitConnections(g, cluster.Storage)
	require.NoError(t, err)

	ctx := context.Background()
	globalID := int64(0)
	err = kv.RunInNewTxn(ctx, cluster.Storage, true, func(_ context.Context, txn kv.Transaction) error {
		id, err := meta.NewMutator(txn).AdvanceGlobalIDs(1000)
		globalID = id + 1000
		return err
	})
	require.NoError(t, err)
	userTableIDNotReusedWhenNeedCheck, err := client.AllocTableIDs(ctx, []*metautil.Table{
		generateMetautilTable("mysql", globalID-1),
		generateMetautilTable("__TiDB_BR_Temporary_mysql", globalID-2),
		generateMetautilTable("mysql", globalID-3),
		generateMetautilTable("__TiDB_BR_Temporary_mysql", globalID-4),
	}, true, false, nil)
	require.NoError(t, err)
	require.False(t, userTableIDNotReusedWhenNeedCheck)
	err = kv.RunInNewTxn(ctx, cluster.Storage, true, func(_ context.Context, txn kv.Transaction) error {
		id, err := meta.NewMutator(txn).AdvanceGlobalIDs(1000)
		globalID = id + 1000
		return err
	})
	require.NoError(t, err)
	userTableIDNotReusedWhenNeedCheck, err = client.AllocTableIDs(ctx, []*metautil.Table{
		generateMetautilTable("mysql", globalID-1, globalID+1),
		generateMetautilTable("test", globalID+2, globalID+3),
	}, true, false, nil)
	require.NoError(t, err)
	require.False(t, userTableIDNotReusedWhenNeedCheck)
	err = kv.RunInNewTxn(ctx, cluster.Storage, true, func(_ context.Context, txn kv.Transaction) error {
		id, err := meta.NewMutator(txn).AdvanceGlobalIDs(1000)
		globalID = id + 1000
		return err
	})
	require.NoError(t, err)
	userTableIDNotReusedWhenNeedCheck, err = client.AllocTableIDs(ctx, []*metautil.Table{
		generateMetautilTable("mysql", globalID-1, globalID+1),
		generateMetautilTable("test", globalID+2, globalID),
		generateMetautilTable("test2", globalID+3, globalID+4),
	}, true, false, nil)
	require.NoError(t, err)
	require.True(t, userTableIDNotReusedWhenNeedCheck)

	tableInfo, err := cluster.Domain.InfoSchema().TableInfoByName(ast.NewCIStr("mysql"), ast.NewCIStr("user"))
	require.NoError(t, err)
	userDownstreamTableID := tableInfo.ID
	tables := []*metautil.Table{
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "user", userDownstreamTableID),
		generateMetautilTableWithName("test", "user", 100),
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "test", 200),
		generateMetautilTableWithName("mysql", "test", 300),
	}
	userTableIDNotReusedWhenNeedCheck, err = client.AllocTableIDs(ctx, tables, false, true, &checkpoint.PreallocIDs{
		Start:          1,
		ReusableBorder: 10000,
		End:            20000,
		Hash:           computeIDsHash([]int64{userDownstreamTableID, 100, 200, 300}),
	})
	require.NoError(t, err)
	require.False(t, userTableIDNotReusedWhenNeedCheck)
	newTables := client.CleanTablesIfTemporarySystemTablesRenamed(false, true, tables)
	require.True(t, mustNoTable(newTables, "mysql", "user"))
	require.Len(t, newTables, 3)

	tableInfo, err = cluster.Domain.InfoSchema().TableInfoByName(ast.NewCIStr("mysql"), ast.NewCIStr("stats_meta"))
	require.NoError(t, err)
	statsMetaDownstreamTableID := tableInfo.ID
	tables = []*metautil.Table{
		generateMetautilTableWithName("test", "stats_meta", 100),
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "stats_meta", statsMetaDownstreamTableID),
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "test", 200),
		generateMetautilTableWithName("mysql", "test", 300),
	}
	userTableIDNotReusedWhenNeedCheck, err = client.AllocTableIDs(ctx, tables, true, false, &checkpoint.PreallocIDs{
		Start:          1,
		ReusableBorder: 10000,
		End:            20000,
		Hash:           computeIDsHash([]int64{statsMetaDownstreamTableID, 100, 200, 300}),
	})
	require.NoError(t, err)
	require.False(t, userTableIDNotReusedWhenNeedCheck)
	newTables = client.CleanTablesIfTemporarySystemTablesRenamed(true, false, tables)
	require.True(t, mustNoTable(newTables, "mysql", "stats_meta"))
	require.Len(t, newTables, 3)

	tables = []*metautil.Table{
		generateMetautilTableWithName("test", "stats_meta", 100),
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "stats_meta", statsMetaDownstreamTableID),
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "test", 200),
		generateMetautilTableWithName("__TiDB_BR_Temporary_mysql", "user", userDownstreamTableID),
		generateMetautilTableWithName("mysql", "test", 300),
	}
	userTableIDNotReusedWhenNeedCheck, err = client.AllocTableIDs(ctx, tables, true, true, &checkpoint.PreallocIDs{
		Start:          1,
		ReusableBorder: 10000,
		End:            20000,
		Hash:           computeIDsHash([]int64{statsMetaDownstreamTableID, userDownstreamTableID, 100, 200, 300}),
	})
	require.NoError(t, err)
	require.False(t, userTableIDNotReusedWhenNeedCheck)
	newTables = client.CleanTablesIfTemporarySystemTablesRenamed(true, true, tables)
	require.True(t, mustNoTable(newTables, "mysql", "stats_meta"))
	require.True(t, mustNoTable(newTables, "mysql", "user"))
	require.Len(t, newTables, 3)
}

func mustNoTable(tables []*metautil.Table, dbName, tableName string) bool {
	for _, table := range tables {
		if table.DB.Name.O == dbName && table.Info.Name.O == tableName {
			return false
		}
	}
	return true
}

func computeIDsHash(ids []int64) [32]byte {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	buffer := make([]byte, 8)

	for _, id := range ids {
		binary.BigEndian.PutUint64(buffer, uint64(id))
		_, err := h.Write(buffer)
		if err != nil {
			panic(errors.Wrapf(err, "failed to write table ID %d to hash", id))
		}
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func TestGetMinUserTableID(t *testing.T) {
	minUserTableID := snapclient.GetMinUserTableID([]*metautil.Table{
		generateMetautilTable("mysql", 1),
		generateMetautilTable("__TiDB_BR_Temporary_mysql", 2),
		generateMetautilTable("mysql", 4),
		generateMetautilTable("__TiDB_BR_Temporary_mysql", 3),
	})
	require.Equal(t, int64(math.MaxInt64), minUserTableID)
	minUserTableID = snapclient.GetMinUserTableID([]*metautil.Table{
		generateMetautilTable("mysql", 3, 1),
		generateMetautilTable("test", 4, 6),
	})
	require.Equal(t, int64(4), minUserTableID)
	minUserTableID = snapclient.GetMinUserTableID([]*metautil.Table{
		generateMetautilTable("mysql", 4, 1),
		generateMetautilTable("test", 3, 2),
		generateMetautilTable("test2", 5, 6),
	})
	require.Equal(t, int64(2), minUserTableID)
}
