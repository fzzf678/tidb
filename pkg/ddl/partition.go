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

package ddl

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	field_types "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
)

const (
	partitionMaxValue = "MAXVALUE"
)

func checkAddPartition(jobCtx *jobContext, job *model.Job) (*model.TableInfo, *model.PartitionInfo, []model.PartitionDefinition, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	args := jobCtx.jobArgs.(*model.TablePartitionArgs)
	partInfo := args.PartInfo
	if len(tblInfo.Partition.AddingDefinitions) > 0 {
		return tblInfo, partInfo, tblInfo.Partition.AddingDefinitions, nil
	}
	return tblInfo, partInfo, []model.PartitionDefinition{}, nil
}

// TODO: Move this into reorganize partition!
func (w *worker) onAddTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	// Handle the rolling back job
	if job.IsRollingback() {
		ver, err := w.rollbackLikeDropPartition(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// notice: addingDefinitions is empty when job is in state model.StateNone
	tblInfo, partInfo, addingDefinitions, err := checkAddPartition(jobCtx, job)
	if err != nil {
		return ver, err
	}

	// In order to skip maintaining the state check in partitionDefinition, TiDB use addingDefinition instead of state field.
	// So here using `job.SchemaState` to judge what the stage of this job is.
	switch job.SchemaState {
	case model.StateNone:
		// job.SchemaState == model.StateNone means the job is in the initial state of add partition.
		// Here should use partInfo from job directly and do some check action.
		err = checkAddPartitionTooManyPartitions(uint64(len(tblInfo.Partition.Definitions) + len(partInfo.Definitions)))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkAddPartitionValue(tblInfo, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkAddPartitionNameUnique(tblInfo, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		// move the adding definition into tableInfo.
		updateAddingPartitionInfo(partInfo, tblInfo)
		tblInfo.Partition.DDLState = model.StateReplicaOnly
		tblInfo.Partition.DDLAction = job.Type
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// modify placement settings
		for _, def := range tblInfo.Partition.AddingDefinitions {
			if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(jobCtx.metaMut, job, def.PlacementPolicyRef); err != nil {
				return ver, errors.Trace(err)
			}
		}

		if tblInfo.TiFlashReplica != nil {
			// Must set placement rule, and make sure it succeeds.
			if err := infosync.ConfigureTiFlashPDForPartitions(true, &tblInfo.Partition.AddingDefinitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); err != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				return ver, errors.Trace(err)
			}
		}

		_, err = alterTablePartitionBundles(jobCtx.metaMut, tblInfo, tblInfo.Partition.AddingDefinitions)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		ids := getIDs([]*model.TableInfo{tblInfo})
		for _, p := range tblInfo.Partition.AddingDefinitions {
			ids = append(ids, p.ID)
		}
		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, ids); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		// none -> replica only
		job.SchemaState = model.StateReplicaOnly
	case model.StateReplicaOnly:
		// replica only -> public
		failpoint.Inject("sleepBeforeReplicaOnly", func(val failpoint.Value) {
			sleepSecond := val.(int)
			time.Sleep(time.Duration(sleepSecond) * time.Second)
		})
		// Here need do some tiflash replica complement check.
		// TODO: If a table is with no TiFlashReplica or it is not available, the replica-only state can be eliminated.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait it's replica to
			// be finished. Otherwise the query to this partition will be blocked.
			needRetry, err := checkPartitionReplica(tblInfo.TiFlashReplica.Count, addingDefinitions, jobCtx)
			if err != nil {
				return convertAddTablePartitionJob2RollbackJob(jobCtx, job, err, tblInfo)
			}
			if needRetry {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckTiDBHTTPAPIHalfInterval)
				// Set the error here which will lead this job exit when it's retry times beyond the limitation.
				return ver, errors.Errorf("[ddl] add partition wait for tiflash replica to complete")
			}
		}

		// When TiFlash Replica is ready, we must move them into `AvailablePartitionIDs`.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			for _, d := range partInfo.Definitions {
				tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, d.ID)
				err = infosync.UpdateTiFlashProgressCache(d.ID, 1)
				if err != nil {
					// just print log, progress will be updated in `refreshTiFlashTicker`
					logutil.DDLLogger().Error("update tiflash sync progress cache failed",
						zap.Error(err),
						zap.Int64("tableID", tblInfo.ID),
						zap.Int64("partitionID", d.ID),
					)
				}
			}
		}
		// For normal and replica finished table, move the `addingDefinitions` into `Definitions`.
		updatePartitionInfo(tblInfo)
		var scatterScope string
		if val, ok := job.GetSystemVars(vardef.TiDBScatterRegion); ok {
			scatterScope = val
		}
		preSplitAndScatter(w.sess.Context, jobCtx.store, tblInfo, addingDefinitions, scatterScope)

		tblInfo.Partition.DDLState = model.StateNone
		tblInfo.Partition.DDLAction = model.ActionNone
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		addPartitionEvent := notifier.NewAddPartitionEvent(tblInfo, partInfo)
		err = asyncNotifyEvent(jobCtx, addPartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

// alterTableLabelRule updates Label Rules if they exists
// returns true if changed.
func alterTableLabelRule(schemaName string, meta *model.TableInfo, ids []int64) (bool, error) {
	tableRuleID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, schemaName, meta.Name.L)
	oldRule, err := infosync.GetLabelRules(context.TODO(), []string{tableRuleID})
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(oldRule) == 0 {
		return false, nil
	}

	r, ok := oldRule[tableRuleID]
	if ok {
		rule := r.Reset(schemaName, meta.Name.L, "", ids...)
		err = infosync.PutLabelRule(context.TODO(), rule)
		if err != nil {
			return false, errors.Wrapf(err, "failed to notify PD label rule")
		}
		return true, nil
	}
	return false, nil
}

func alterTablePartitionBundles(t *meta.Mutator, tblInfo *model.TableInfo, addDefs []model.PartitionDefinition) (bool, error) {
	// We want to achieve:
	// - before we do any reorganization/write to new partitions/global indexes that the placement rules are in-place
	// - not removing any placement rules for removed partitions
	// So we will:
	// 1) First write the new bundles including both new and old partitions,
	//    EXCEPT if the old partition is in fact a table, then skip that partition
	// 2) Then overwrite the bundles with the final partitioning scheme (second call in onReorg/

	tblInfo = tblInfo.Clone()
	p := tblInfo.Partition
	if p != nil {
		// if partitioning a non-partitioned table, we will first change the metadata,
		// so the table looks like a partitioned table, with the first/only partition having
		// the same partition ID as the table, so we can access the table as a single partition.
		// But in this case we should not add a bundle rule for the same range
		// both as table and partition.
		if p.Definitions[0].ID != tblInfo.ID {
			// prepend with existing partitions
			addDefs = append(p.Definitions, addDefs...)
		}
		p.Definitions = addDefs
	}

	// bundle for table should be recomputed because it includes some default configs for partitions
	tblBundle, err := placement.NewTableBundle(t, tblInfo)
	if err != nil {
		return false, errors.Trace(err)
	}

	var bundles []*placement.Bundle
	if tblBundle != nil {
		bundles = append(bundles, tblBundle)
	}

	partitionBundles, err := placement.NewPartitionListBundles(t, addDefs)
	if err != nil {
		return false, errors.Trace(err)
	}

	bundles = append(bundles, partitionBundles...)

	if len(bundles) > 0 {
		return true, infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	}
	return false, nil
}

// When drop/truncate a partition, we should still keep the dropped partition's placement settings to avoid unnecessary region schedules.
// When a partition is not configured with a placement policy directly, its rule is in the table's placement group which will be deleted after
// partition truncated/dropped. So it is necessary to create a standalone placement group with partition id after it.
func droppedPartitionBundles(t *meta.Mutator, tblInfo *model.TableInfo, dropPartitions []model.PartitionDefinition) ([]*placement.Bundle, error) {
	partitions := make([]model.PartitionDefinition, 0, len(dropPartitions))
	for _, def := range dropPartitions {
		def = def.Clone()
		if def.PlacementPolicyRef == nil {
			def.PlacementPolicyRef = tblInfo.PlacementPolicyRef
		}

		if def.PlacementPolicyRef != nil {
			partitions = append(partitions, def)
		}
	}

	return placement.NewPartitionListBundles(t, partitions)
}

// updatePartitionInfo merge `addingDefinitions` into `Definitions` in the tableInfo.
func updatePartitionInfo(tblInfo *model.TableInfo) {
	parInfo := &model.PartitionInfo{}
	oldDefs, newDefs := tblInfo.Partition.Definitions, tblInfo.Partition.AddingDefinitions
	parInfo.Definitions = make([]model.PartitionDefinition, 0, len(newDefs)+len(oldDefs))
	parInfo.Definitions = append(parInfo.Definitions, oldDefs...)
	parInfo.Definitions = append(parInfo.Definitions, newDefs...)
	tblInfo.Partition.Definitions = parInfo.Definitions
	tblInfo.Partition.AddingDefinitions = nil
}

// updateAddingPartitionInfo write adding partitions into `addingDefinitions` field in the tableInfo.
func updateAddingPartitionInfo(partitionInfo *model.PartitionInfo, tblInfo *model.TableInfo) {
	newDefs := partitionInfo.Definitions
	tblInfo.Partition.AddingDefinitions = make([]model.PartitionDefinition, 0, len(newDefs))
	tblInfo.Partition.AddingDefinitions = append(tblInfo.Partition.AddingDefinitions, newDefs...)
}

// removePartitionAddingDefinitionsFromTableInfo remove the `addingDefinitions` in the tableInfo.
func removePartitionAddingDefinitionsFromTableInfo(tblInfo *model.TableInfo) ([]int64, []string) {
	physicalTableIDs := make([]int64, 0, len(tblInfo.Partition.AddingDefinitions))
	partNames := make([]string, 0, len(tblInfo.Partition.AddingDefinitions))
	for _, one := range tblInfo.Partition.AddingDefinitions {
		physicalTableIDs = append(physicalTableIDs, one.ID)
		partNames = append(partNames, one.Name.L)
	}
	tblInfo.Partition.AddingDefinitions = nil
	return physicalTableIDs, partNames
}

// checkAddPartitionValue check add Partition Values,
// For Range: values less than value must be strictly increasing for each partition.
// For List: if a Default partition exists,
//
//	no ADD partition can be allowed
//	(needs reorganize partition instead).
func checkAddPartitionValue(meta *model.TableInfo, part *model.PartitionInfo) error {
	switch meta.Partition.Type {
	case ast.PartitionTypeRange:
		if len(meta.Partition.Columns) == 0 {
			newDefs, oldDefs := part.Definitions, meta.Partition.Definitions
			rangeValue := oldDefs[len(oldDefs)-1].LessThan[0]
			if strings.EqualFold(rangeValue, "MAXVALUE") {
				return errors.Trace(dbterror.ErrPartitionMaxvalue)
			}

			currentRangeValue, err := strconv.Atoi(rangeValue)
			if err != nil {
				return errors.Trace(err)
			}

			for i := range newDefs {
				ifMaxvalue := strings.EqualFold(newDefs[i].LessThan[0], "MAXVALUE")
				if ifMaxvalue && i == len(newDefs)-1 {
					return nil
				} else if ifMaxvalue && i != len(newDefs)-1 {
					return errors.Trace(dbterror.ErrPartitionMaxvalue)
				}

				nextRangeValue, err := strconv.Atoi(newDefs[i].LessThan[0])
				if err != nil {
					return errors.Trace(err)
				}
				if nextRangeValue <= currentRangeValue {
					return errors.Trace(dbterror.ErrRangeNotIncreasing)
				}
				currentRangeValue = nextRangeValue
			}
		}
	case ast.PartitionTypeList:
		if meta.Partition.GetDefaultListPartition() != -1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("ADD List partition, already contains DEFAULT partition. Please use REORGANIZE PARTITION instead")
		}
	}
	return nil
}

func checkPartitionReplica(replicaCount uint64, addingDefinitions []model.PartitionDefinition, jobCtx *jobContext) (needWait bool, err error) {
	failpoint.Inject("mockWaitTiFlashReplica", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(true, nil)
		}
	})
	failpoint.Inject("mockWaitTiFlashReplicaOK", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(false, nil)
		}
	})

	ctx := context.Background()
	pdCli := jobCtx.store.(tikv.Storage).GetRegionCache().PDClient()
	stores, err := pdCli.GetAllStores(ctx)
	if err != nil {
		return needWait, errors.Trace(err)
	}
	// Check whether stores have `count` tiflash engines.
	tiFlashStoreCount := uint64(0)
	for _, store := range stores {
		if storeHasEngineTiFlashLabel(store) {
			tiFlashStoreCount++
		}
	}
	if replicaCount > tiFlashStoreCount {
		return false, errors.Errorf("[ddl] the tiflash replica count: %d should be less than the total tiflash server count: %d", replicaCount, tiFlashStoreCount)
	}
	for _, pDef := range addingDefinitions {
		startKey, endKey := tablecodec.GetTableHandleKeyRange(pDef.ID)
		regions, err := pdCli.BatchScanRegions(ctx, []router.KeyRange{{StartKey: startKey, EndKey: endKey}}, -1, opt.WithAllowFollowerHandle())
		if err != nil {
			return needWait, errors.Trace(err)
		}
		// For every region in the partition, if it has some corresponding peers and
		// no pending peers, that means the replication has completed.
		for _, region := range regions {
			tiflashPeerAtLeastOne := checkTiFlashPeerStoreAtLeastOne(stores, region.Meta.Peers)
			failpoint.Inject("ForceTiflashNotAvailable", func(v failpoint.Value) {
				tiflashPeerAtLeastOne = v.(bool)
			})
			// It's unnecessary to wait all tiflash peer to be replicated.
			// Here only make sure that tiflash peer count > 0 (at least one).
			if tiflashPeerAtLeastOne {
				continue
			}
			needWait = true
			logutil.DDLLogger().Info("partition replicas check failed in replica-only DDL state", zap.Int64("pID", pDef.ID), zap.Uint64("wait region ID", region.Meta.Id), zap.Bool("tiflash peer at least one", tiflashPeerAtLeastOne), zap.Time("check time", time.Now()))
			return needWait, nil
		}
	}
	logutil.DDLLogger().Info("partition replicas check ok in replica-only DDL state")
	return needWait, nil
}

func checkTiFlashPeerStoreAtLeastOne(stores []*metapb.Store, peers []*metapb.Peer) bool {
	for _, peer := range peers {
		for _, store := range stores {
			if peer.StoreId == store.Id && storeHasEngineTiFlashLabel(store) {
				return true
			}
		}
	}
	return false
}

func storeHasEngineTiFlashLabel(store *metapb.Store) bool {
	for _, label := range store.Labels {
		if label.Key == placement.EngineLabelKey && label.Value == placement.EngineLabelTiFlash {
			return true
		}
	}
	return false
}

func checkListPartitions(defs []*ast.PartitionDefinition) error {
	for _, def := range defs {
		_, ok := def.Clause.(*ast.PartitionDefinitionClauseIn)
		if !ok {
			switch def.Clause.(type) {
			case *ast.PartitionDefinitionClauseLessThan:
				return ast.ErrPartitionWrongValues.GenWithStackByArgs("RANGE", "LESS THAN")
			case *ast.PartitionDefinitionClauseNone:
				return ast.ErrPartitionRequiresValues.GenWithStackByArgs("LIST", "IN")
			default:
				return dbterror.ErrUnsupportedCreatePartition.GenWithStack("Only VALUES IN () is supported for LIST partitioning")
			}
		}
	}
	return nil
}

// buildTablePartitionInfo builds partition info and checks for some errors.
func buildTablePartitionInfo(ctx *metabuild.Context, s *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if s == nil {
		return nil
	}

	var enable bool
	switch s.Tp {
	case ast.PartitionTypeRange:
		enable = true
	case ast.PartitionTypeList:
		enable = true
		err := checkListPartitions(s.Definitions)
		if err != nil {
			return err
		}
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		// Partition by hash and key is enabled by default.
		if s.Sub != nil {
			// Subpartitioning only allowed with Range or List
			return ast.ErrSubpartition
		}
		// Note that linear hash is simply ignored, and creates non-linear hash/key.
		if s.Linear {
			ctx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.FastGen(fmt.Sprintf("LINEAR %s is not supported, using non-linear %s instead", s.Tp.String(), s.Tp.String())))
		}
		if s.Tp == ast.PartitionTypeHash || len(s.ColumnNames) != 0 {
			enable = true
		}
		if s.Tp == ast.PartitionTypeKey && len(s.ColumnNames) == 0 {
			enable = true
		}
	}

	if !enable {
		ctx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.FastGen(fmt.Sprintf("Unsupported partition type %v, treat as normal table", s.Tp)))
		return nil
	}
	if s.Sub != nil {
		ctx.AppendWarning(dbterror.ErrUnsupportedCreatePartition.FastGen(fmt.Sprintf("Unsupported subpartitioning, only using %v partitioning", s.Tp)))
	}

	pi := &model.PartitionInfo{
		Type:   s.Tp,
		Enable: enable,
		Num:    s.Num,
	}
	tbInfo.Partition = pi
	if s.Expr != nil {
		if err := checkPartitionFuncValid(ctx.GetExprCtx(), tbInfo, s.Expr); err != nil {
			return errors.Trace(err)
		}
		buf := new(bytes.Buffer)
		restoreFlags := format.DefaultRestoreFlags | format.RestoreBracketAroundBinaryOperation |
			format.RestoreWithoutSchemaName | format.RestoreWithoutTableName
		restoreCtx := format.NewRestoreCtx(restoreFlags, buf)
		if err := s.Expr.Restore(restoreCtx); err != nil {
			return err
		}
		pi.Expr = buf.String()
	} else if s.ColumnNames != nil {
		pi.Columns = make([]ast.CIStr, 0, len(s.ColumnNames))
		for _, cn := range s.ColumnNames {
			pi.Columns = append(pi.Columns, cn.Name)
		}
		if pi.Type == ast.PartitionTypeKey && len(s.ColumnNames) == 0 {
			if tbInfo.PKIsHandle {
				pi.Columns = append(pi.Columns, tbInfo.GetPkName())
				pi.IsEmptyColumns = true
			} else if key := tbInfo.GetPrimaryKey(); key != nil {
				for _, col := range key.Columns {
					pi.Columns = append(pi.Columns, col.Name)
				}
				pi.IsEmptyColumns = true
			}
		}
		if err := checkColumnsPartitionType(tbInfo); err != nil {
			return err
		}
	}

	exprCtx := ctx.GetExprCtx()
	err := generatePartitionDefinitionsFromInterval(exprCtx, s, tbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	defs, err := buildPartitionDefinitionsInfo(exprCtx, s.Definitions, tbInfo, s.Num)
	if err != nil {
		return errors.Trace(err)
	}

	tbInfo.Partition.Definitions = defs

	if len(s.UpdateIndexes) > 0 {
		updateIndexes := make([]model.UpdateIndexInfo, 0, len(s.UpdateIndexes))
		dupCheck := make(map[string]struct{})
		for _, idxUpdate := range s.UpdateIndexes {
			idxOffset := -1
			for i := range tbInfo.Indices {
				if strings.EqualFold(tbInfo.Indices[i].Name.L, idxUpdate.Name) {
					idxOffset = i
					break
				}
			}
			if idxOffset == -1 {
				if strings.EqualFold("primary", idxUpdate.Name) &&
					tbInfo.PKIsHandle {
					return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
				}
				return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(idxUpdate.Name)
			}
			if _, ok := dupCheck[strings.ToLower(idxUpdate.Name)]; ok {
				return dbterror.ErrWrongNameForIndex.GenWithStackByArgs(idxUpdate.Name)
			}
			dupCheck[strings.ToLower(idxUpdate.Name)] = struct{}{}
			if idxUpdate.Option != nil && idxUpdate.Option.Global {
				tbInfo.Indices[idxOffset].Global = true
			} else {
				tbInfo.Indices[idxOffset].Global = false
			}
			updateIndexes = append(updateIndexes, model.UpdateIndexInfo{IndexName: idxUpdate.Name, Global: tbInfo.Indices[idxOffset].Global})
			tbInfo.Partition.DDLUpdateIndexes = updateIndexes
		}
	}

	for _, index := range tbInfo.Indices {
		if index.Unique {
			ck, err := checkPartitionKeysConstraint(pi, index.Columns, tbInfo)
			if err != nil {
				return err
			}
			if !ck {
				if index.Primary && tbInfo.IsCommonHandle {
					return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
				}
				if !index.Global {
					return dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs(index.Name.O)
				}
			}
		}
	}
	if tbInfo.PKIsHandle {
		// This case is covers when the Handle is the PK (only ints), since it would not
		// have an entry in the tblInfo.Indices
		indexCols := []*model.IndexColumn{{
			Name:   tbInfo.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		ck, err := checkPartitionKeysConstraint(pi, indexCols, tbInfo)
		if err != nil {
			return err
		}
		if !ck {
			return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
		}
	}

	return nil
}

func rewritePartitionQueryString(ctx sessionctx.Context, s *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if s == nil {
		return nil
	}

	if s.Interval != nil {
		// Syntactic sugar for INTERVAL partitioning
		// Generate the resulting CREATE TABLE as the query string
		query, ok := ctx.Value(sessionctx.QueryString).(string)
		if ok {
			sqlMode := ctx.GetSessionVars().SQLMode
			var buf bytes.Buffer
			AppendPartitionDefs(tbInfo.Partition, &buf, sqlMode)

			syntacticSugar := s.Interval.OriginalText()
			syntacticStart := strings.Index(query, syntacticSugar)
			if syntacticStart == -1 {
				logutil.DDLLogger().Error("Can't find INTERVAL definition in prepare stmt",
					zap.String("INTERVAL definition", syntacticSugar), zap.String("prepare stmt", query))
				return errors.Errorf("Can't find INTERVAL definition in PREPARE STMT")
			}
			newQuery := query[:syntacticStart] + "(" + buf.String() + ")" + query[syntacticStart+len(syntacticSugar):]
			ctx.SetValue(sessionctx.QueryString, newQuery)
		}
	}
	return nil
}

func getPartitionColSlices(sctx expression.BuildContext, tblInfo *model.TableInfo, s *ast.PartitionOptions) (partCols stringSlice, err error) {
	if s.Expr != nil {
		extractCols := newPartitionExprChecker(sctx, tblInfo)
		s.Expr.Accept(extractCols)
		partColumns, err := extractCols.columns, extractCols.err
		if err != nil {
			return nil, err
		}
		return columnInfoSlice(partColumns), nil
	} else if len(s.ColumnNames) > 0 {
		return columnNameSlice(s.ColumnNames), nil
	} else if len(s.ColumnNames) == 0 {
		if tblInfo.PKIsHandle {
			return columnInfoSlice([]*model.ColumnInfo{tblInfo.GetPkColInfo()}), nil
		} else if key := tblInfo.GetPrimaryKey(); key != nil {
			colInfos := make([]*model.ColumnInfo, 0, len(key.Columns))
			for _, col := range key.Columns {
				colInfos = append(colInfos, model.FindColumnInfo(tblInfo.Cols(), col.Name.L))
			}
			return columnInfoSlice(colInfos), nil
		}
	}
	return nil, errors.Errorf("Table partition metadata not correct, neither partition expression or list of partition columns")
}

func checkColumnsPartitionType(tbInfo *model.TableInfo) error {
	for _, col := range tbInfo.Partition.Columns {
		colInfo := tbInfo.FindPublicColumnByName(col.L)
		if colInfo == nil {
			return errors.Trace(dbterror.ErrFieldNotFoundPart)
		}
		if !isColTypeAllowedAsPartitioningCol(tbInfo.Partition.Type, colInfo.FieldType) {
			return dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.O)
		}
	}
	return nil
}

func isValidKeyPartitionColType(fieldType types.FieldType) bool {
	switch fieldType.GetType() {
	case mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeJSON, mysql.TypeGeometry, mysql.TypeTiDBVectorFloat32:
		return false
	default:
		return true
	}
}

func isColTypeAllowedAsPartitioningCol(partType ast.PartitionType, fieldType types.FieldType) bool {
	// For key partition, the permitted partition field types can be all field types except
	// BLOB, JSON, Geometry
	if partType == ast.PartitionTypeKey {
		return isValidKeyPartitionColType(fieldType)
	}
	// The permitted data types are shown in the following list:
	// All integer types
	// DATE and DATETIME
	// CHAR, VARCHAR, BINARY, and VARBINARY
	// See https://dev.mysql.com/doc/mysql-partitioning-excerpt/5.7/en/partitioning-columns.html
	// Note that also TIME is allowed in MySQL. Also see https://bugs.mysql.com/bug.php?id=84362
	switch fieldType.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeDuration:
	case mysql.TypeVarchar, mysql.TypeString:
	default:
		return false
	}
	return true
}

// getPartitionIntervalFromTable checks if a partitioned table matches a generated INTERVAL partitioned scheme
// will return nil if error occurs, i.e. not an INTERVAL partitioned table
func getPartitionIntervalFromTable(ctx expression.BuildContext, tbInfo *model.TableInfo) *ast.PartitionInterval {
	if tbInfo.Partition == nil ||
		tbInfo.Partition.Type != ast.PartitionTypeRange {
		return nil
	}
	if len(tbInfo.Partition.Columns) > 1 {
		// Multi-column RANGE COLUMNS is not supported with INTERVAL
		return nil
	}
	if len(tbInfo.Partition.Definitions) < 2 {
		// Must have at least two partitions to calculate an INTERVAL
		return nil
	}

	var (
		interval  ast.PartitionInterval
		startIdx  = 0
		endIdx    = len(tbInfo.Partition.Definitions) - 1
		isIntType = true
		minVal    = "0"
	)
	if len(tbInfo.Partition.Columns) > 0 {
		partCol := findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol.FieldType.EvalType() == types.ETInt {
			minv := getLowerBoundInt(partCol)
			minVal = strconv.FormatInt(minv, 10)
		} else if partCol.FieldType.EvalType() == types.ETDatetime {
			isIntType = false
			minVal = "0000-01-01"
		} else {
			// Only INT and Datetime columns are supported for INTERVAL partitioning
			return nil
		}
	} else {
		if !isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo) {
			minVal = "-9223372036854775808"
		}
	}

	// Check if possible null partition
	firstPartLessThan := driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[0].LessThan[0])
	if strings.EqualFold(firstPartLessThan, minVal) {
		interval.NullPart = true
		startIdx++
		firstPartLessThan = driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[startIdx].LessThan[0])
	}
	// flag if MAXVALUE partition
	lastPartLessThan := driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[endIdx].LessThan[0])
	if strings.EqualFold(lastPartLessThan, partitionMaxValue) {
		interval.MaxValPart = true
		endIdx--
		lastPartLessThan = driver.UnwrapFromSingleQuotes(tbInfo.Partition.Definitions[endIdx].LessThan[0])
	}
	// Guess the interval
	if startIdx >= endIdx {
		// Must have at least two partitions to calculate an INTERVAL
		return nil
	}
	var firstExpr, lastExpr ast.ExprNode
	if isIntType {
		exprStr := fmt.Sprintf("((%s) - (%s)) DIV %d", lastPartLessThan, firstPartLessThan, endIdx-startIdx)
		expr, err := expression.ParseSimpleExpr(ctx, exprStr)
		if err != nil {
			return nil
		}
		val, isNull, err := expr.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if isNull || err != nil || val < 1 {
			// If NULL, error or interval < 1 then cannot be an INTERVAL partitioned table
			return nil
		}
		interval.IntervalExpr.Expr = ast.NewValueExpr(val, "", "")
		interval.IntervalExpr.TimeUnit = ast.TimeUnitInvalid
		firstExpr, err = astIntValueExprFromStr(firstPartLessThan, minVal == "0")
		if err != nil {
			return nil
		}
		interval.FirstRangeEnd = &firstExpr
		lastExpr, err = astIntValueExprFromStr(lastPartLessThan, minVal == "0")
		if err != nil {
			return nil
		}
		interval.LastRangeEnd = &lastExpr
	} else { // types.ETDatetime
		exprStr := fmt.Sprintf("TIMESTAMPDIFF(SECOND, '%s', '%s')", firstPartLessThan, lastPartLessThan)
		expr, err := expression.ParseSimpleExpr(ctx, exprStr)
		if err != nil {
			return nil
		}
		val, isNull, err := expr.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if isNull || err != nil || val < 1 {
			// If NULL, error or interval < 1 then cannot be an INTERVAL partitioned table
			return nil
		}

		// This will not find all matches > 28 days, since INTERVAL 1 MONTH can generate
		// 2022-01-31, 2022-02-28, 2022-03-31 etc. so we just assume that if there is a
		// diff >= 28 days, we will try with Month and not retry with something else...
		i := val / int64(endIdx-startIdx)
		if i < (28 * 24 * 60 * 60) {
			// Since it is not stored or displayed, non need to try Minute..Week!
			interval.IntervalExpr.Expr = ast.NewValueExpr(i, "", "")
			interval.IntervalExpr.TimeUnit = ast.TimeUnitSecond
		} else {
			// Since it is not stored or displayed, non need to try to match Quarter or Year!
			if (endIdx - startIdx) <= 3 {
				// in case February is in the range
				i = i / (28 * 24 * 60 * 60)
			} else {
				// This should be good for intervals up to 5 years
				i = i / (30 * 24 * 60 * 60)
			}
			interval.IntervalExpr.Expr = ast.NewValueExpr(i, "", "")
			interval.IntervalExpr.TimeUnit = ast.TimeUnitMonth
		}

		firstExpr = ast.NewValueExpr(firstPartLessThan, "", "")
		lastExpr = ast.NewValueExpr(lastPartLessThan, "", "")
		interval.FirstRangeEnd = &firstExpr
		interval.LastRangeEnd = &lastExpr
	}

	partitionMethod := ast.PartitionMethod{
		Tp:       ast.PartitionTypeRange,
		Interval: &interval,
	}
	partOption := &ast.PartitionOptions{PartitionMethod: partitionMethod}
	// Generate the definitions from interval, first and last
	err := generatePartitionDefinitionsFromInterval(ctx, partOption, tbInfo)
	if err != nil {
		return nil
	}

	return &interval
}

// comparePartitionAstAndModel compares a generated *ast.PartitionOptions and a *model.PartitionInfo
func comparePartitionAstAndModel(ctx expression.BuildContext, pAst *ast.PartitionOptions, pModel *model.PartitionInfo, partCol *model.ColumnInfo) error {
	a := pAst.Definitions
	m := pModel.Definitions
	if len(pAst.Definitions) != len(pModel.Definitions) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: number of partitions generated != partition defined (%d != %d)", len(a), len(m))
	}

	evalCtx := ctx.GetEvalCtx()
	evalFn := func(expr ast.ExprNode) (types.Datum, error) {
		val, err := expression.EvalSimpleAst(ctx, ast.NewValueExpr(expr, "", ""))
		if err != nil || partCol == nil {
			return val, err
		}
		return val.ConvertTo(evalCtx.TypeCtx(), &partCol.FieldType)
	}
	for i := range pAst.Definitions {
		// Allow options to differ! (like Placement Rules)
		// Allow names to differ!

		// Check MAXVALUE
		maxVD := false
		if strings.EqualFold(m[i].LessThan[0], partitionMaxValue) {
			maxVD = true
		}
		generatedExpr := a[i].Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs[0]
		_, maxVG := generatedExpr.(*ast.MaxValueExpr)
		if maxVG || maxVD {
			if maxVG && maxVD {
				continue
			}
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("INTERVAL partitioning: MAXVALUE clause defined for partition %s differs between generated and defined", m[i].Name.O))
		}

		lessThan := m[i].LessThan[0]
		if len(lessThan) > 1 && lessThan[:1] == "'" && lessThan[len(lessThan)-1:] == "'" {
			lessThan = driver.UnwrapFromSingleQuotes(lessThan)
		}
		lessThanVal, err := evalFn(ast.NewValueExpr(lessThan, "", ""))
		if err != nil {
			return err
		}
		generatedExprVal, err := evalFn(generatedExpr)
		if err != nil {
			return err
		}
		cmp, err := lessThanVal.Compare(evalCtx.TypeCtx(), &generatedExprVal, collate.GetBinaryCollator())
		if err != nil {
			return err
		}
		if cmp != 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("INTERVAL partitioning: LESS THAN for partition %s differs between generated and defined", m[i].Name.O))
		}
	}
	return nil
}

// comparePartitionDefinitions check if generated definitions are the same as the given ones
// Allow names to differ
// returns error in case of error or non-accepted difference
func comparePartitionDefinitions(ctx expression.BuildContext, a, b []*ast.PartitionDefinition) error {
	if len(a) != len(b) {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("number of partitions generated != partition defined (%d != %d)", len(a), len(b))
	}
	for i := range a {
		if len(b[i].Sub) > 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s does have unsupported subpartitions", b[i].Name.O))
		}
		// TODO: We could extend the syntax to allow for table options too, like:
		// CREATE TABLE t ... INTERVAL ... LAST PARTITION LESS THAN ('2015-01-01') PLACEMENT POLICY = 'cheapStorage'
		// ALTER TABLE t LAST PARTITION LESS THAN ('2022-01-01') PLACEMENT POLICY 'defaultStorage'
		// ALTER TABLE t LAST PARTITION LESS THAN ('2023-01-01') PLACEMENT POLICY 'fastStorage'
		if len(b[i].Options) > 0 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s does have unsupported options", b[i].Name.O))
		}
		lessThan, ok := b[i].Clause.(*ast.PartitionDefinitionClauseLessThan)
		if !ok {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s does not have the right type for LESS THAN", b[i].Name.O))
		}
		definedExpr := lessThan.Exprs[0]
		generatedExpr := a[i].Clause.(*ast.PartitionDefinitionClauseLessThan).Exprs[0]
		_, maxVD := definedExpr.(*ast.MaxValueExpr)
		_, maxVG := generatedExpr.(*ast.MaxValueExpr)
		if maxVG || maxVD {
			if maxVG && maxVD {
				continue
			}
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s differs between generated and defined for MAXVALUE", b[i].Name.O))
		}
		cmpExpr := &ast.BinaryOperationExpr{
			Op: opcode.EQ,
			L:  definedExpr,
			R:  generatedExpr,
		}
		cmp, err := expression.EvalSimpleAst(ctx, cmpExpr)
		if err != nil {
			return err
		}
		if cmp.GetInt64() != 1 {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(fmt.Sprintf("partition %s differs between generated and defined for expression", b[i].Name.O))
		}
	}
	return nil
}

func getLowerBoundInt(partCols ...*model.ColumnInfo) int64 {
	ret := int64(0)
	for _, col := range partCols {
		if mysql.HasUnsignedFlag(col.FieldType.GetFlag()) {
			return 0
		}
		ret = min(ret, types.IntegerSignedLowerBound(col.GetType()))
	}
	return ret
}

// generatePartitionDefinitionsFromInterval generates partition Definitions according to INTERVAL options on partOptions
func generatePartitionDefinitionsFromInterval(ctx expression.BuildContext, partOptions *ast.PartitionOptions, tbInfo *model.TableInfo) error {
	if partOptions.Interval == nil {
		return nil
	}
	if tbInfo.Partition.Type != ast.PartitionTypeRange {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, only allowed on RANGE partitioning")
	}
	if len(partOptions.ColumnNames) > 1 || len(tbInfo.Partition.Columns) > 1 {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, does not allow RANGE COLUMNS with more than one column")
	}
	var partCol *model.ColumnInfo
	if len(tbInfo.Partition.Columns) > 0 {
		partCol = findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, could not find any RANGE COLUMNS")
		}
		// Only support Datetime, date and INT column types for RANGE INTERVAL!
		switch partCol.FieldType.EvalType() {
		case types.ETInt, types.ETDatetime:
		default:
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, only supports Date, Datetime and INT types")
		}
	}
	// Allow given partition definitions, but check it later!
	definedPartDefs := partOptions.Definitions
	partOptions.Definitions = make([]*ast.PartitionDefinition, 0, 1)
	if partOptions.Interval.FirstRangeEnd == nil || partOptions.Interval.LastRangeEnd == nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, currently requires FIRST and LAST partitions to be defined")
	}
	switch partOptions.Interval.IntervalExpr.TimeUnit {
	case ast.TimeUnitInvalid, ast.TimeUnitYear, ast.TimeUnitQuarter, ast.TimeUnitMonth, ast.TimeUnitWeek, ast.TimeUnitDay, ast.TimeUnitHour, ast.TimeUnitMinute, ast.TimeUnitSecond:
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning, only supports YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE and SECOND as time unit")
	}
	first := ast.PartitionDefinitionClauseLessThan{
		Exprs: []ast.ExprNode{*partOptions.Interval.FirstRangeEnd},
	}
	last := ast.PartitionDefinitionClauseLessThan{
		Exprs: []ast.ExprNode{*partOptions.Interval.LastRangeEnd},
	}
	if len(tbInfo.Partition.Columns) > 0 {
		colTypes := collectColumnsType(tbInfo)
		if len(colTypes) != len(tbInfo.Partition.Columns) {
			return dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
		}
		if _, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, first.Exprs); err != nil {
			return err
		}
		if _, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, last.Exprs); err != nil {
			return err
		}
	} else {
		if err := checkPartitionValuesIsInt(ctx, "FIRST PARTITION", first.Exprs, tbInfo); err != nil {
			return err
		}
		if err := checkPartitionValuesIsInt(ctx, "LAST PARTITION", last.Exprs, tbInfo); err != nil {
			return err
		}
	}
	if partOptions.Interval.NullPart {
		var partExpr ast.ExprNode
		if len(tbInfo.Partition.Columns) == 1 && partOptions.Interval.IntervalExpr.TimeUnit != ast.TimeUnitInvalid {
			// Notice compatibility with MySQL, keyword here is 'supported range' but MySQL seems to work from 0000-01-01 too
			// https://dev.mysql.com/doc/refman/8.0/en/datetime.html says range 1000-01-01 - 9999-12-31
			// https://docs.pingcap.com/tidb/dev/data-type-date-and-time says The supported range is '0000-01-01' to '9999-12-31'
			// set LESS THAN to ZeroTime
			partExpr = ast.NewValueExpr("0000-01-01", "", "")
		} else {
			var minv int64
			if partCol != nil {
				minv = getLowerBoundInt(partCol)
			} else {
				if !isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo) {
					minv = math.MinInt64
				}
			}
			partExpr = ast.NewValueExpr(minv, "", "")
		}
		partOptions.Definitions = append(partOptions.Definitions, &ast.PartitionDefinition{
			Name: ast.NewCIStr("P_NULL"),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{partExpr},
			},
		})
	}

	err := GeneratePartDefsFromInterval(ctx, ast.AlterTablePartition, tbInfo, partOptions)
	if err != nil {
		return err
	}

	if partOptions.Interval.MaxValPart {
		partOptions.Definitions = append(partOptions.Definitions, &ast.PartitionDefinition{
			Name: ast.NewCIStr("P_MAXVALUE"),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{&ast.MaxValueExpr{}},
			},
		})
	}

	if len(definedPartDefs) > 0 {
		err := comparePartitionDefinitions(ctx, partOptions.Definitions, definedPartDefs)
		if err != nil {
			return err
		}
		// Seems valid, so keep the defined so that the user defined names are kept etc.
		partOptions.Definitions = definedPartDefs
	} else if len(tbInfo.Partition.Definitions) > 0 {
		err := comparePartitionAstAndModel(ctx, partOptions, tbInfo.Partition, partCol)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkAndGetColumnsTypeAndValuesMatch(ctx expression.BuildContext, colTypes []types.FieldType, exprs []ast.ExprNode) ([]types.Datum, error) {
	// Validate() has already checked len(colNames) = len(exprs)
	// create table ... partition by range columns (cols)
	// partition p0 values less than (expr)
	// check the type of cols[i] and expr is consistent.
	valDatums := make([]types.Datum, 0, len(colTypes))
	for i, colExpr := range exprs {
		if _, ok := colExpr.(*ast.MaxValueExpr); ok {
			valDatums = append(valDatums, types.NewStringDatum(partitionMaxValue))
			continue
		}
		if d, ok := colExpr.(*ast.DefaultExpr); ok {
			if d.Name != nil {
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
			continue
		}
		colType := colTypes[i]
		val, err := expression.EvalSimpleAst(ctx, colExpr)
		if err != nil {
			return nil, err
		}
		// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
		vkind := val.Kind()
		switch colType.GetType() {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeDuration:
			switch vkind {
			case types.KindString, types.KindBytes, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			switch vkind {
			case types.KindInt64, types.KindUint64, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeFloat, mysql.TypeDouble:
			switch vkind {
			case types.KindFloat32, types.KindFloat64, types.KindNull:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		case mysql.TypeString, mysql.TypeVarString:
			switch vkind {
			case types.KindString, types.KindBytes, types.KindNull, types.KindBinaryLiteral:
			default:
				return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
			}
		}
		evalCtx := ctx.GetEvalCtx()
		newVal, err := val.ConvertTo(evalCtx.TypeCtx(), &colType)
		if err != nil {
			return nil, dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
		}
		valDatums = append(valDatums, newVal)
	}
	return valDatums, nil
}

func astIntValueExprFromStr(s string, unsigned bool) (ast.ExprNode, error) {
	if unsigned {
		u, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return ast.NewValueExpr(u, "", ""), nil
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, err
	}
	return ast.NewValueExpr(i, "", ""), nil
}

// GeneratePartDefsFromInterval generates range partitions from INTERVAL partitioning.
// Handles
//   - CREATE TABLE: all partitions are generated
//   - ALTER TABLE FIRST PARTITION (expr): Drops all partitions before the partition matching the expr (i.e. sets that partition as the new first partition)
//     i.e. will return the partitions from old FIRST partition to (and including) new FIRST partition
//   - ALTER TABLE LAST PARTITION (expr): Creates new partitions from (excluding) old LAST partition to (including) new LAST partition
//
// partition definitions will be set on partitionOptions
func GeneratePartDefsFromInterval(ctx expression.BuildContext, tp ast.AlterTableType, tbInfo *model.TableInfo, partitionOptions *ast.PartitionOptions) error {
	if partitionOptions == nil {
		return nil
	}
	var sb strings.Builder
	err := partitionOptions.Interval.IntervalExpr.Expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	if err != nil {
		return err
	}
	intervalString := driver.UnwrapFromSingleQuotes(sb.String())
	if len(intervalString) < 1 || intervalString[:1] < "1" || intervalString[:1] > "9" {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL, should be a positive number")
	}
	var currVal types.Datum
	var startExpr, lastExpr, currExpr ast.ExprNode
	var timeUnit ast.TimeUnitType
	var partCol *model.ColumnInfo
	if len(tbInfo.Partition.Columns) == 1 {
		partCol = findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
		if partCol == nil {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL COLUMNS partitioning: could not find partitioning column")
		}
	}
	timeUnit = partitionOptions.Interval.IntervalExpr.TimeUnit
	switch tp {
	case ast.AlterTablePartition:
		// CREATE TABLE
		startExpr = *partitionOptions.Interval.FirstRangeEnd
		lastExpr = *partitionOptions.Interval.LastRangeEnd
	case ast.AlterTableDropFirstPartition:
		startExpr = *partitionOptions.Interval.FirstRangeEnd
		lastExpr = partitionOptions.Expr
	case ast.AlterTableAddLastPartition:
		startExpr = *partitionOptions.Interval.LastRangeEnd
		lastExpr = partitionOptions.Expr
	default:
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: Internal error during generating altered INTERVAL partitions, no known alter type")
	}
	lastVal, err := expression.EvalSimpleAst(ctx, lastExpr)
	if err != nil {
		return err
	}
	evalCtx := ctx.GetEvalCtx()
	if partCol != nil {
		lastVal, err = lastVal.ConvertTo(evalCtx.TypeCtx(), &partCol.FieldType)
		if err != nil {
			return err
		}
	}
	partDefs := partitionOptions.Definitions
	if len(partDefs) == 0 {
		partDefs = make([]*ast.PartitionDefinition, 0, 1)
	}
	for i := range mysql.PartitionCountLimit {
		if i == 0 {
			currExpr = startExpr
			// TODO: adjust the startExpr and have an offset for interval to handle
			// Month/Quarters with start partition on day 28/29/30
			if tp == ast.AlterTableAddLastPartition {
				// ALTER TABLE LAST PARTITION ...
				// Current LAST PARTITION/start already exists, skip to next partition
				continue
			}
		} else {
			currExpr = &ast.BinaryOperationExpr{
				Op: opcode.Mul,
				L:  ast.NewValueExpr(i, "", ""),
				R:  partitionOptions.Interval.IntervalExpr.Expr,
			}
			if timeUnit == ast.TimeUnitInvalid {
				currExpr = &ast.BinaryOperationExpr{
					Op: opcode.Plus,
					L:  startExpr,
					R:  currExpr,
				}
			} else {
				currExpr = &ast.FuncCallExpr{
					FnName: ast.NewCIStr("DATE_ADD"),
					Args: []ast.ExprNode{
						startExpr,
						currExpr,
						&ast.TimeUnitExpr{Unit: timeUnit},
					},
				}
			}
		}
		currVal, err = expression.EvalSimpleAst(ctx, currExpr)
		if err != nil {
			return err
		}
		if partCol != nil {
			currVal, err = currVal.ConvertTo(evalCtx.TypeCtx(), &partCol.FieldType)
			if err != nil {
				return err
			}
		}
		cmp, err := currVal.Compare(evalCtx.TypeCtx(), &lastVal, collate.GetBinaryCollator())
		if err != nil {
			return err
		}
		if cmp > 0 {
			lastStr, err := lastVal.ToString()
			if err != nil {
				return err
			}
			sb.Reset()
			err = startExpr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
			if err != nil {
				return err
			}
			startStr := sb.String()
			errStr := fmt.Sprintf("INTERVAL: expr (%s) not matching FIRST + n INTERVALs (%s + n * %s",
				lastStr, startStr, intervalString)
			if timeUnit != ast.TimeUnitInvalid {
				errStr = errStr + " " + timeUnit.String()
			}
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs(errStr + ")")
		}
		valStr, err := currVal.ToString()
		if err != nil {
			return err
		}
		if len(valStr) == 0 || valStr[0:1] == "'" {
			return dbterror.ErrGeneralUnsupportedDDL.GenWithStackByArgs("INTERVAL partitioning: Error when generating partition values")
		}
		partName := "P_LT_" + valStr
		if timeUnit != ast.TimeUnitInvalid {
			currExpr = ast.NewValueExpr(valStr, "", "")
		} else {
			if valStr[:1] == "-" {
				currExpr = ast.NewValueExpr(currVal.GetInt64(), "", "")
			} else {
				currExpr = ast.NewValueExpr(currVal.GetUint64(), "", "")
			}
		}
		partDefs = append(partDefs, &ast.PartitionDefinition{
			Name: ast.NewCIStr(partName),
			Clause: &ast.PartitionDefinitionClauseLessThan{
				Exprs: []ast.ExprNode{currExpr},
			},
		})
		if cmp == 0 {
			// Last partition!
			break
		}
		// The last loop still not reach the max value, return error.
		if i == mysql.PartitionCountLimit-1 {
			return errors.Trace(dbterror.ErrTooManyPartitions)
		}
	}
	if len(tbInfo.Partition.Definitions)+len(partDefs) > mysql.PartitionCountLimit {
		return errors.Trace(dbterror.ErrTooManyPartitions)
	}
	partitionOptions.Definitions = partDefs
	return nil
}

// buildPartitionDefinitionsInfo build partition definitions info without assign partition id. tbInfo will be constant
func buildPartitionDefinitionsInfo(ctx expression.BuildContext, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo, numParts uint64) (partitions []model.PartitionDefinition, err error) {
	switch tbInfo.Partition.Type {
	case ast.PartitionTypeNone:
		if len(defs) != 1 {
			return nil, dbterror.ErrUnsupportedPartitionType
		}
		partitions = []model.PartitionDefinition{{Name: defs[0].Name}}
		if comment, set := defs[0].Comment(); set {
			partitions[0].Comment = comment
		}
	case ast.PartitionTypeRange:
		partitions, err = buildRangePartitionDefinitions(ctx, defs, tbInfo)
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		partitions, err = buildHashPartitionDefinitions(defs, tbInfo, numParts)
	case ast.PartitionTypeList:
		partitions, err = buildListPartitionDefinitions(ctx, defs, tbInfo)
	default:
		err = dbterror.ErrUnsupportedPartitionType
	}

	if err != nil {
		return nil, err
	}

	return partitions, nil
}

func setPartitionPlacementFromOptions(partition *model.PartitionDefinition, options []*ast.TableOption) error {
	// the partition inheritance of placement rules don't have to copy the placement elements to themselves.
	// For example:
	// t placement policy x (p1 placement policy y, p2)
	// p2 will share the same rule as table t does, but it won't copy the meta to itself. we will
	// append p2 range to the coverage of table t's rules. This mechanism is good for cascading change
	// when policy x is altered.
	for _, opt := range options {
		if opt.Tp == ast.TableOptionPlacementPolicy {
			partition.PlacementPolicyRef = &model.PolicyRefInfo{
				Name: ast.NewCIStr(opt.StrValue),
			}
		}
	}

	return nil
}

func isNonDefaultPartitionOptionsUsed(defs []model.PartitionDefinition) bool {
	for i := range defs {
		orgDef := defs[i]
		if orgDef.Name.O != fmt.Sprintf("p%d", i) {
			return true
		}
		if len(orgDef.Comment) > 0 {
			return true
		}
		if orgDef.PlacementPolicyRef != nil {
			return true
		}
	}
	return false
}

func buildHashPartitionDefinitions(defs []*ast.PartitionDefinition, tbInfo *model.TableInfo, numParts uint64) ([]model.PartitionDefinition, error) {
	if err := checkAddPartitionTooManyPartitions(tbInfo.Partition.Num); err != nil {
		return nil, err
	}

	definitions := make([]model.PartitionDefinition, numParts)
	oldParts := uint64(len(tbInfo.Partition.Definitions))
	for i := range numParts {
		if i < oldParts {
			// Use the existing definitions
			def := tbInfo.Partition.Definitions[i]
			definitions[i].Name = def.Name
			definitions[i].Comment = def.Comment
			definitions[i].PlacementPolicyRef = def.PlacementPolicyRef
		} else if i < oldParts+uint64(len(defs)) {
			// Use the new defs
			def := defs[i-oldParts]
			definitions[i].Name = def.Name
			definitions[i].Comment, _ = def.Comment()
			if err := setPartitionPlacementFromOptions(&definitions[i], def.Options); err != nil {
				return nil, err
			}
		} else {
			// Use the default
			definitions[i].Name = ast.NewCIStr(fmt.Sprintf("p%d", i))
		}
	}
	return definitions, nil
}

func buildListPartitionDefinitions(ctx expression.BuildContext, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	colTypes := collectColumnsType(tbInfo)
	if len(colTypes) != len(tbInfo.Partition.Columns) {
		return nil, dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
	}
	for _, def := range defs {
		if err := def.Clause.Validate(ast.PartitionTypeList, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseIn)
		partVals := make([][]types.Datum, 0, len(clause.Values))
		if len(tbInfo.Partition.Columns) > 0 {
			for _, vs := range clause.Values {
				vals, err := checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, vs)
				if err != nil {
					return nil, err
				}
				partVals = append(partVals, vals)
			}
		} else {
			for _, vs := range clause.Values {
				if err := checkPartitionValuesIsInt(ctx, def.Name, vs, tbInfo); err != nil {
					return nil, err
				}
			}
		}
		comment, _ := def.Comment()
		err := checkTooLongTable(def.Name)
		if err != nil {
			return nil, err
		}
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			Comment: comment,
		}

		if err = setPartitionPlacementFromOptions(&piDef, def.Options); err != nil {
			return nil, err
		}

		buf := new(bytes.Buffer)
		for valIdx, vs := range clause.Values {
			inValue := make([]string, 0, len(vs))
			isDefault := false
			if len(vs) == 1 {
				if _, ok := vs[0].(*ast.DefaultExpr); ok {
					isDefault = true
				}
			}
			if len(partVals) > valIdx && !isDefault {
				for colIdx := range partVals[valIdx] {
					partVal, err := generatePartValuesWithTp(partVals[valIdx][colIdx], colTypes[colIdx])
					if err != nil {
						return nil, err
					}
					inValue = append(inValue, partVal)
				}
			} else {
				for i := range vs {
					vs[i].Accept(exprChecker)
					if exprChecker.err != nil {
						return nil, exprChecker.err
					}
					buf.Reset()
					vs[i].Format(buf)
					inValue = append(inValue, buf.String())
				}
			}
			piDef.InValues = append(piDef.InValues, inValue)
			buf.Reset()
		}
		definitions = append(definitions, piDef)
	}
	return definitions, nil
}

func collectColumnsType(tbInfo *model.TableInfo) []types.FieldType {
	if len(tbInfo.Partition.Columns) > 0 {
		colTypes := make([]types.FieldType, 0, len(tbInfo.Partition.Columns))
		for _, col := range tbInfo.Partition.Columns {
			c := findColumnByName(col.L, tbInfo)
			if c == nil {
				return nil
			}
			colTypes = append(colTypes, c.FieldType)
		}

		return colTypes
	}

	return nil
}

func buildRangePartitionDefinitions(ctx expression.BuildContext, defs []*ast.PartitionDefinition, tbInfo *model.TableInfo) ([]model.PartitionDefinition, error) {
	definitions := make([]model.PartitionDefinition, 0, len(defs))
	exprChecker := newPartitionExprChecker(ctx, nil, checkPartitionExprAllowed)
	colTypes := collectColumnsType(tbInfo)
	if len(colTypes) != len(tbInfo.Partition.Columns) {
		return nil, dbterror.ErrWrongPartitionName.GenWithStack("partition column name cannot be found")
	}
	for _, def := range defs {
		if err := def.Clause.Validate(ast.PartitionTypeRange, len(tbInfo.Partition.Columns)); err != nil {
			return nil, err
		}
		clause := def.Clause.(*ast.PartitionDefinitionClauseLessThan)
		var partValDatums []types.Datum
		if len(tbInfo.Partition.Columns) > 0 {
			var err error
			if partValDatums, err = checkAndGetColumnsTypeAndValuesMatch(ctx, colTypes, clause.Exprs); err != nil {
				return nil, err
			}
		} else {
			if err := checkPartitionValuesIsInt(ctx, def.Name, clause.Exprs, tbInfo); err != nil {
				return nil, err
			}
		}
		comment, _ := def.Comment()
		evalCtx := ctx.GetEvalCtx()
		comment, err := validateCommentLength(evalCtx.ErrCtx(), evalCtx.SQLMode(), def.Name.L, &comment, dbterror.ErrTooLongTablePartitionComment)
		if err != nil {
			return nil, err
		}
		err = checkTooLongTable(def.Name)
		if err != nil {
			return nil, err
		}
		piDef := model.PartitionDefinition{
			Name:    def.Name,
			Comment: comment,
		}

		if err = setPartitionPlacementFromOptions(&piDef, def.Options); err != nil {
			return nil, err
		}

		buf := new(bytes.Buffer)
		// Range columns partitions support multi-column partitions.
		for i, expr := range clause.Exprs {
			expr.Accept(exprChecker)
			if exprChecker.err != nil {
				return nil, exprChecker.err
			}
			// If multi-column use new evaluated+normalized output, instead of just formatted expression
			if len(partValDatums) > i {
				var partVal string
				if partValDatums[i].Kind() == types.KindNull {
					return nil, dbterror.ErrNullInValuesLessThan
				}
				if _, ok := clause.Exprs[i].(*ast.MaxValueExpr); ok {
					partVal, err = partValDatums[i].ToString()
					if err != nil {
						return nil, err
					}
				} else {
					partVal, err = generatePartValuesWithTp(partValDatums[i], colTypes[i])
					if err != nil {
						return nil, err
					}
				}

				piDef.LessThan = append(piDef.LessThan, partVal)
			} else {
				expr.Format(buf)
				piDef.LessThan = append(piDef.LessThan, buf.String())
				buf.Reset()
			}
		}
		definitions = append(definitions, piDef)
	}
	return definitions, nil
}

func checkPartitionValuesIsInt(ctx expression.BuildContext, defName any, exprs []ast.ExprNode, tbInfo *model.TableInfo) error {
	tp := types.NewFieldType(mysql.TypeLonglong)
	if isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo) {
		tp.AddFlag(mysql.UnsignedFlag)
	}
	for _, exp := range exprs {
		if _, ok := exp.(*ast.MaxValueExpr); ok {
			continue
		}
		if d, ok := exp.(*ast.DefaultExpr); ok {
			if d.Name != nil {
				return dbterror.ErrPartitionConstDomain.GenWithStackByArgs()
			}
			continue
		}
		val, err := expression.EvalSimpleAst(ctx, exp)
		if err != nil {
			return err
		}
		switch val.Kind() {
		case types.KindUint64, types.KindNull:
		case types.KindInt64:
			if mysql.HasUnsignedFlag(tp.GetFlag()) && val.GetInt64() < 0 {
				return dbterror.ErrPartitionConstDomain.GenWithStackByArgs()
			}
		default:
			return dbterror.ErrValuesIsNotIntType.GenWithStackByArgs(defName)
		}

		evalCtx := ctx.GetEvalCtx()
		_, err = val.ConvertTo(evalCtx.TypeCtx(), tp)
		if err != nil && !types.ErrOverflow.Equal(err) {
			return dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
		}
	}

	return nil
}

func checkPartitionNameUnique(pi *model.PartitionInfo) error {
	newPars := pi.Definitions
	partNames := make(map[string]struct{}, len(newPars))
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

func checkAddPartitionNameUnique(tbInfo *model.TableInfo, pi *model.PartitionInfo) error {
	partNames := make(map[string]struct{})
	if tbInfo.Partition != nil {
		oldPars := tbInfo.Partition.Definitions
		for _, oldPar := range oldPars {
			partNames[oldPar.Name.L] = struct{}{}
		}
	}
	newPars := pi.Definitions
	for _, newPar := range newPars {
		if _, ok := partNames[newPar.Name.L]; ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(newPar.Name)
		}
		partNames[newPar.Name.L] = struct{}{}
	}
	return nil
}

func checkReorgPartitionNames(p *model.PartitionInfo, droppedNames []string, pi *model.PartitionInfo) error {
	partNames := make(map[string]struct{})
	oldDefs := p.Definitions
	for _, oldDef := range oldDefs {
		partNames[oldDef.Name.L] = struct{}{}
	}
	for _, delName := range droppedNames {
		droppedName := strings.ToLower(delName)
		if _, ok := partNames[droppedName]; !ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(delName)
		}
		delete(partNames, droppedName)
	}
	newDefs := pi.Definitions
	for _, newDef := range newDefs {
		if _, ok := partNames[newDef.Name.L]; ok {
			return dbterror.ErrSameNamePartition.GenWithStackByArgs(newDef.Name)
		}
		partNames[newDef.Name.L] = struct{}{}
	}
	return nil
}

func checkAndOverridePartitionID(newTableInfo, oldTableInfo *model.TableInfo) error {
	// If any old partitionInfo has lost, that means the partition ID lost too, so did the data, repair failed.
	if newTableInfo.Partition == nil {
		return nil
	}
	if oldTableInfo.Partition == nil {
		return dbterror.ErrRepairTableFail.GenWithStackByArgs("Old table doesn't have partitions")
	}
	if newTableInfo.Partition.Type != oldTableInfo.Partition.Type {
		return dbterror.ErrRepairTableFail.GenWithStackByArgs("Partition type should be the same")
	}
	// Check whether partitionType is hash partition.
	if newTableInfo.Partition.Type == ast.PartitionTypeHash {
		if newTableInfo.Partition.Num != oldTableInfo.Partition.Num {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Hash partition num should be the same")
		}
	}
	for i, newOne := range newTableInfo.Partition.Definitions {
		found := false
		for _, oldOne := range oldTableInfo.Partition.Definitions {
			// Fix issue 17952 which wanna substitute partition range expr.
			// So eliminate stringSliceEqual(newOne.LessThan, oldOne.LessThan) here.
			if newOne.Name.L == oldOne.Name.L {
				newTableInfo.Partition.Definitions[i].ID = oldOne.ID
				found = true
				break
			}
		}
		if !found {
			return dbterror.ErrRepairTableFail.GenWithStackByArgs("Partition " + newOne.Name.L + " has lost")
		}
	}
	return nil
}

// checkPartitionFuncValid checks partition function validly.
func checkPartitionFuncValid(ctx expression.BuildContext, tblInfo *model.TableInfo, expr ast.ExprNode) error {
	if expr == nil {
		return nil
	}
	exprChecker := newPartitionExprChecker(ctx, tblInfo, checkPartitionExprArgs, checkPartitionExprAllowed)
	expr.Accept(exprChecker)
	if exprChecker.err != nil {
		return errors.Trace(exprChecker.err)
	}
	if len(exprChecker.columns) == 0 {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}
	return nil
}

// checkResultOK derives from https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc
// For partition tables, mysql do not support Constant, random or timezone-dependent expressions
// Based on mysql code to check whether field is valid, every time related type has check_valid_arguments_processor function.
func checkResultOK(ok bool) error {
	if !ok {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}

	return nil
}

// checkPartitionFuncType checks partition function return type.
func checkPartitionFuncType(ctx expression.BuildContext, anyExpr any, schema string, tblInfo *model.TableInfo) error {
	if anyExpr == nil {
		return nil
	}
	if schema == "" {
		schema = ctx.GetEvalCtx().CurrentDB()
	}
	var e expression.Expression
	var err error
	switch expr := anyExpr.(type) {
	case string:
		if expr == "" {
			return nil
		}
		e, err = expression.ParseSimpleExpr(ctx, expr, expression.WithTableInfo(schema, tblInfo))
	case ast.ExprNode:
		e, err = expression.BuildSimpleExpr(ctx, expr, expression.WithTableInfo(schema, tblInfo))
	default:
		return errors.Trace(dbterror.ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION"))
	}
	if err != nil {
		return errors.Trace(err)
	}
	if e.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		return nil
	}
	if col, ok := e.(*expression.Column); ok {
		if col2, ok2 := anyExpr.(*ast.ColumnNameExpr); ok2 {
			return errors.Trace(dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col2.Name.Name.L))
		}
		return errors.Trace(dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(col.OrigName))
	}
	return errors.Trace(dbterror.ErrPartitionFuncNotAllowed.GenWithStackByArgs("PARTITION"))
}

// checkRangePartitionValue checks whether `less than value` is strictly increasing for each partition.
// Side effect: it may simplify the partition range definition from a constant expression to an integer.
func checkRangePartitionValue(ctx expression.BuildContext, tblInfo *model.TableInfo) error {
	pi := tblInfo.Partition
	defs := pi.Definitions
	if len(defs) == 0 {
		return nil
	}

	if strings.EqualFold(defs[len(defs)-1].LessThan[0], partitionMaxValue) {
		defs = defs[:len(defs)-1]
	}
	isUnsigned := isPartExprUnsigned(ctx.GetEvalCtx(), tblInfo)
	var prevRangeValue any
	for i := range defs {
		if strings.EqualFold(defs[i].LessThan[0], partitionMaxValue) {
			return errors.Trace(dbterror.ErrPartitionMaxvalue)
		}

		currentRangeValue, fromExpr, err := getRangeValue(ctx, defs[i].LessThan[0], isUnsigned)
		if err != nil {
			return errors.Trace(err)
		}
		if fromExpr {
			// Constant fold the expression.
			defs[i].LessThan[0] = fmt.Sprintf("%d", currentRangeValue)
		}

		if i == 0 {
			prevRangeValue = currentRangeValue
			continue
		}

		if isUnsigned {
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				return errors.Trace(dbterror.ErrRangeNotIncreasing)
			}
		} else {
			if currentRangeValue.(int64) <= prevRangeValue.(int64) {
				return errors.Trace(dbterror.ErrRangeNotIncreasing)
			}
		}
		prevRangeValue = currentRangeValue
	}
	return nil
}

func checkListPartitionValue(ctx expression.BuildContext, tblInfo *model.TableInfo) error {
	pi := tblInfo.Partition
	if len(pi.Definitions) == 0 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("LIST")
	}
	expStr, err := formatListPartitionValue(ctx, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	partitionsValuesMap := make(map[string]struct{})
	for _, s := range expStr {
		if _, ok := partitionsValuesMap[s]; ok {
			return errors.Trace(dbterror.ErrMultipleDefConstInListPart)
		}
		partitionsValuesMap[s] = struct{}{}
	}

	return nil
}

func formatListPartitionValue(ctx expression.BuildContext, tblInfo *model.TableInfo) ([]string, error) {
	defs := tblInfo.Partition.Definitions
	pi := tblInfo.Partition
	var colTps []*types.FieldType
	cols := make([]*model.ColumnInfo, 0, len(pi.Columns))
	if len(pi.Columns) == 0 {
		tp := types.NewFieldType(mysql.TypeLonglong)
		if isPartExprUnsigned(ctx.GetEvalCtx(), tblInfo) {
			tp.AddFlag(mysql.UnsignedFlag)
		}
		colTps = []*types.FieldType{tp}
	} else {
		colTps = make([]*types.FieldType, 0, len(pi.Columns))
		for _, colName := range pi.Columns {
			colInfo := findColumnByName(colName.L, tblInfo)
			if colInfo == nil {
				return nil, errors.Trace(dbterror.ErrFieldNotFoundPart)
			}
			colTps = append(colTps, colInfo.FieldType.Clone())
			cols = append(cols, colInfo)
		}
	}

	haveDefault := false
	exprStrs := make([]string, 0)
	inValueStrs := make([]string, 0, max(len(pi.Columns), 1))
	for i := range defs {
	inValuesLoop:
		for j, vs := range defs[i].InValues {
			inValueStrs = inValueStrs[:0]
			for k, v := range vs {
				// if DEFAULT would be given as string, like "DEFAULT",
				// it would be stored as "'DEFAULT'",
				if strings.EqualFold(v, "DEFAULT") && k == 0 && len(vs) == 1 {
					if haveDefault {
						return nil, dbterror.ErrMultipleDefConstInListPart
					}
					haveDefault = true
					continue inValuesLoop
				}
				if strings.EqualFold(v, "MAXVALUE") {
					return nil, errors.Trace(dbterror.ErrMaxvalueInValuesIn)
				}
				expr, err := expression.ParseSimpleExpr(ctx, v, expression.WithCastExprTo(colTps[k]))
				if err != nil {
					return nil, errors.Trace(err)
				}
				eval, err := expr.Eval(ctx.GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil, errors.Trace(err)
				}
				s, err := eval.ToString()
				if err != nil {
					return nil, errors.Trace(err)
				}
				if eval.IsNull() {
					s = "NULL"
				} else {
					if colTps[k].EvalType() == types.ETInt {
						defs[i].InValues[j][k] = s
					}
					if colTps[k].EvalType() == types.ETString {
						s = string(hack.String(collate.GetCollator(cols[k].GetCollate()).Key(s)))
						s = driver.WrapInSingleQuotes(s)
					}
				}
				inValueStrs = append(inValueStrs, s)
			}
			exprStrs = append(exprStrs, strings.Join(inValueStrs, ","))
		}
	}
	return exprStrs, nil
}

// getRangeValue gets an integer from the range value string.
// The returned boolean value indicates whether the input string is a constant expression.
func getRangeValue(ctx expression.BuildContext, str string, unsigned bool) (any, bool, error) {
	// Unsigned bigint was converted to uint64 handle.
	if unsigned {
		if value, err := strconv.ParseUint(str, 10, 64); err == nil {
			return value, false, nil
		}

		e, err1 := expression.ParseSimpleExpr(ctx, str)
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err2 == nil && !isNull {
			return uint64(res), true, nil
		}
	} else {
		if value, err := strconv.ParseInt(str, 10, 64); err == nil {
			return value, false, nil
		}
		// The range value maybe not an integer, it could be a constant expression.
		// For example, the following two cases are the same:
		// PARTITION p0 VALUES LESS THAN (TO_SECONDS('2004-01-01'))
		// PARTITION p0 VALUES LESS THAN (63340531200)
		e, err1 := expression.ParseSimpleExpr(ctx, str)
		if err1 != nil {
			return 0, false, err1
		}
		res, isNull, err2 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err2 == nil && !isNull {
			return res, true, nil
		}
	}
	return 0, false, dbterror.ErrNotAllowedTypeInPartition.GenWithStackByArgs(str)
}

// CheckDropTablePartition checks if the partition exists and does not allow deleting the last existing partition in the table.
func CheckDropTablePartition(meta *model.TableInfo, partLowerNames []string) error {
	pi := meta.Partition
	if pi.Type != ast.PartitionTypeRange && pi.Type != ast.PartitionTypeList {
		return dbterror.ErrOnlyOnRangeListPartition.GenWithStackByArgs("DROP")
	}

	// To be error compatible with MySQL, we need to do this first!
	// see https://github.com/pingcap/tidb/issues/31681#issuecomment-1015536214
	oldDefs := pi.Definitions
	if len(oldDefs) <= len(partLowerNames) {
		return errors.Trace(dbterror.ErrDropLastPartition)
	}

	dupCheck := make(map[string]bool)
	for _, pn := range partLowerNames {
		found := false
		for _, def := range oldDefs {
			if def.Name.L == pn {
				if _, ok := dupCheck[pn]; ok {
					return errors.Trace(dbterror.ErrDropPartitionNonExistent.GenWithStackByArgs("DROP"))
				}
				dupCheck[pn] = true
				found = true
				break
			}
		}
		if !found {
			return errors.Trace(dbterror.ErrDropPartitionNonExistent.GenWithStackByArgs("DROP"))
		}
	}
	return nil
}

// updateDroppingPartitionInfo move dropping partitions to DroppingDefinitions
func updateDroppingPartitionInfo(tblInfo *model.TableInfo, partLowerNames []string) {
	oldDefs := tblInfo.Partition.Definitions
	newDefs := make([]model.PartitionDefinition, 0, len(oldDefs)-len(partLowerNames))
	droppingDefs := make([]model.PartitionDefinition, 0, len(partLowerNames))

	// consider using a map to probe partLowerNames if too many partLowerNames
	for i := range oldDefs {
		found := slices.Contains(partLowerNames, oldDefs[i].Name.L)
		if found {
			droppingDefs = append(droppingDefs, oldDefs[i])
		} else {
			newDefs = append(newDefs, oldDefs[i])
		}
	}

	tblInfo.Partition.Definitions = newDefs
	tblInfo.Partition.DroppingDefinitions = droppingDefs
}

func getPartitionDef(tblInfo *model.TableInfo, partName string) (index int, def *model.PartitionDefinition, _ error) {
	defs := tblInfo.Partition.Definitions
	for i := range defs {
		if strings.EqualFold(defs[i].Name.L, strings.ToLower(partName)) {
			return i, &(defs[i]), nil
		}
	}
	return index, nil, table.ErrUnknownPartition.GenWithStackByArgs(partName, tblInfo.Name.O)
}

func getPartitionIDsFromDefinitions(defs []model.PartitionDefinition) []int64 {
	pids := make([]int64, 0, len(defs))
	for _, def := range defs {
		pids = append(pids, def.ID)
	}
	return pids
}

func hasGlobalIndex(tblInfo *model.TableInfo) bool {
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Global {
			return true
		}
	}
	return false
}

// getTableInfoWithDroppingPartitions builds oldTableInfo including dropping partitions, only used by onDropTablePartition.
func getTableInfoWithDroppingPartitions(t *model.TableInfo) *model.TableInfo {
	p := t.Partition
	nt := t.Clone()
	np := *p
	npd := make([]model.PartitionDefinition, 0, len(p.Definitions)+len(p.DroppingDefinitions))
	npd = append(npd, p.Definitions...)
	npd = append(npd, p.DroppingDefinitions...)
	np.Definitions = npd
	np.DroppingDefinitions = nil
	nt.Partition = &np
	return nt
}

func dropLabelRules(ctx context.Context, schemaName, tableName string, partNames []string) error {
	deleteRules := make([]string, 0, len(partNames))
	for _, partName := range partNames {
		deleteRules = append(deleteRules, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, schemaName, tableName, partName))
	}
	// delete batch rules
	patch := label.NewRulePatch([]*label.Rule{}, deleteRules)
	return infosync.UpdateLabelRules(ctx, patch)
}

// rollbackLikeDropPartition does rollback for Reorganize partition and Add partition.
// It will drop newly created partitions that has not yet been used, including cleaning
// up label rules and bundles as well as changed indexes due to global flag.
func (w *worker) rollbackLikeDropPartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	partInfo := args.PartInfo
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	tblInfo.Partition.DroppingDefinitions = nil
	// Collect table/partition ids to clean up, through args.OldPhysicalTblIDs
	// GC will later also drop matching Placement bundles.
	// If we delete them now, it could lead to non-compliant placement or failure during flashback
	physicalTableIDs, pNames := removePartitionAddingDefinitionsFromTableInfo(tblInfo)
	// TODO: Will this drop LabelRules for existing partitions, if the new partitions have the same name?
	err = dropLabelRules(w.ctx, job.SchemaName, tblInfo.Name.L, pNames)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
		job.State = model.JobStateCancelled
		return ver, err
	}
	if partInfo.Type != ast.PartitionTypeNone {
		// ALTER TABLE ... PARTITION BY
		// Also remove anything with the new table id
		if partInfo.NewTableID != 0 {
			physicalTableIDs = append(physicalTableIDs, partInfo.NewTableID)
		}
		// Reset if it was normal table before
		if tblInfo.Partition.Type == ast.PartitionTypeNone ||
			tblInfo.Partition.DDLType == ast.PartitionTypeNone {
			tblInfo.Partition = nil
		}
	}

	var dropIndices []*model.IndexInfo
	for _, indexInfo := range tblInfo.Indices {
		if indexInfo.State == model.StateWriteOnly {
			dropIndices = append(dropIndices, indexInfo)
		}
	}
	var deleteIndices []model.TableIDIndexID
	for _, indexInfo := range dropIndices {
		DropIndexColumnFlag(tblInfo, indexInfo)
		RemoveDependentHiddenColumns(tblInfo, indexInfo)
		removeIndexInfo(tblInfo, indexInfo)
		if indexInfo.Global {
			deleteIndices = append(deleteIndices, model.TableIDIndexID{TableID: tblInfo.ID, IndexID: indexInfo.ID})
		}
		// All other indexes has only been applied to new partitions, that is deleted in whole,
		// including indexes.
	}
	if tblInfo.Partition != nil {
		tblInfo.Partition.ClearReorgIntermediateInfo()
	}

	_, err = alterTablePartitionBundles(metaMut, tblInfo, nil)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}
	ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
	args.OldPhysicalTblIDs = physicalTableIDs
	args.OldGlobalIndexes = deleteIndices
	job.FillFinishedArgs(args)
	return ver, nil
}

// onDropTablePartition deletes old partition meta.
// States in reverse order:
// StateNone
//
//	Old partitions are queued to be deleted (delete_range), global index up-to-date
//
// StateDeleteReorganization
//
//	Old partitions are not accessible/used by any sessions.
//	Inserts/updates of global index which still have entries pointing to old partitions
//	will overwrite those entries.
//	In the background we are reading all old partitions and deleting their entries from
//	the global indexes.
//
// StateDeleteOnly
//
//	Old partitions are no longer visible, but if there is inserts/updates to the global indexes,
//	duplicate key errors will be given, even if the entries are from dropped partitions.
//
// StateWriteOnly
//
//	Old partitions are blocked for read and write. But for read we are allowing
//	"overlapping" partition to be read instead. Which means that write can only
//	happen in the 'overlapping' partitions original range, not into the extended
//	range open by the dropped partitions.
//
// StatePublic
//
//	Original state, unaware of DDL
func (w *worker) onDropTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	partNames := args.PartNames
	metaMut := jobCtx.metaMut
	tblInfo, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch job.SchemaState {
	case model.StatePublic:
		// Here we mark the partitions to be dropped, so they are not read or written
		err = CheckDropTablePartition(tblInfo, partNames)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Reason, see https://github.com/pingcap/tidb/issues/55888
		// Only mark the partitions as to be dropped, so they are not used, but not yet removed.
		originalDefs := tblInfo.Partition.Definitions
		updateDroppingPartitionInfo(tblInfo, partNames)
		tblInfo.Partition.Definitions = originalDefs
		job.SchemaState = model.StateWriteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		tblInfo.Partition.DDLAction = job.Type

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// Since the previous state do not use the dropping partitions,
		// we can now actually remove them, allowing to write into the overlapping range
		// of the higher range partition or LIST default partition.
		updateDroppingPartitionInfo(tblInfo, partNames)
		err = dropLabelRules(jobCtx.stepCtx, job.SchemaName, tblInfo.Name.L, partNames)
		if err != nil {
			// TODO: Add failpoint error/cancel injection and test failure/rollback and cancellation!
			job.State = model.JobStateCancelled
			return ver, errors.Wrapf(err, "failed to notify PD the label rules")
		}

		if _, err := alterTableLabelRule(job.SchemaName, tblInfo, getIDs([]*model.TableInfo{tblInfo})); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		var bundles []*placement.Bundle
		// create placement groups for each dropped partition to keep the data's placement before GC
		// These placements groups will be deleted after GC
		bundles, err = droppedPartitionBundles(metaMut, tblInfo, tblInfo.Partition.DroppingDefinitions)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		var tableBundle *placement.Bundle
		// Recompute table bundle to remove dropped partitions rules from its group
		tableBundle, err = placement.NewTableBundle(metaMut, tblInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		if tableBundle != nil {
			bundles = append(bundles, tableBundle)
		}

		if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		job.SchemaState = model.StateDeleteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteOnly:
		// This state is not a real 'DeleteOnly' state, because tidb does not maintain the state check in partitionDefinition.
		// Insert this state to confirm all servers can not see the old partitions when reorg is running,
		// so that no new data will be inserted into old partitions when reorganizing.
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteReorganization:
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		if hasGlobalIndex(tblInfo) {
			oldTblInfo := getTableInfoWithDroppingPartitions(tblInfo)
			var done bool
			done, err = w.cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx, job, oldTblInfo, physicalTableIDs)
			if err != nil || !done {
				return ver, errors.Trace(err)
			}
		}
		removeTiFlashAvailablePartitionIDs(tblInfo, physicalTableIDs)
		droppedDefs := tblInfo.Partition.DroppingDefinitions
		tblInfo.Partition.DroppingDefinitions = nil
		job.SchemaState = model.StateNone
		tblInfo.Partition.DDLState = job.SchemaState
		tblInfo.Partition.DDLAction = model.ActionNone
		// used by ApplyDiff in updateSchemaVersion
		args.OldPhysicalTblIDs = physicalTableIDs
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		dropPartitionEvent := notifier.NewDropPartitionEvent(
			tblInfo,
			&model.PartitionInfo{Definitions: droppedDefs},
		)
		err = asyncNotifyEvent(jobCtx, dropPartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.SchemaState = model.StateNone
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(args)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}
	return ver, errors.Trace(err)
}

func removeTiFlashAvailablePartitionIDs(tblInfo *model.TableInfo, pids []int64) {
	if tblInfo.TiFlashReplica == nil {
		return
	}
	// Remove the partitions
	ids := tblInfo.TiFlashReplica.AvailablePartitionIDs
	// Rarely called, so OK to take some time, to make it easy
	for _, id := range pids {
		for i, avail := range ids {
			if id == avail {
				tmp := ids[:i]
				tmp = append(tmp, ids[i+1:]...)
				ids = tmp
				break
			}
		}
	}
	tblInfo.TiFlashReplica.AvailablePartitionIDs = ids
}

func replaceTruncatePartitions(job *model.Job, t *meta.Mutator, tblInfo *model.TableInfo, oldIDs, newIDs []int64) (oldDefinitions, newDefinitions []model.PartitionDefinition, err error) {
	oldDefinitions = make([]model.PartitionDefinition, 0, len(oldIDs))
	newDefinitions = make([]model.PartitionDefinition, 0, len(oldIDs))
	pi := tblInfo.Partition
	for i, id := range oldIDs {
		for defIdx := range pi.Definitions {
			// use a reference to actually set the new ID!
			def := &pi.Definitions[defIdx]
			if id == def.ID {
				oldDefinitions = append(oldDefinitions, def.Clone())
				def.ID = newIDs[i]
				// Shallow copy, since we do not need to replace them.
				newDefinitions = append(newDefinitions, *def)
				break
			}
		}
	}

	if err := clearTruncatePartitionTiflashStatus(tblInfo, newDefinitions, oldIDs); err != nil {
		return nil, nil, err
	}

	if err := updateTruncatePartitionLabelRules(job, t, oldDefinitions, newDefinitions, tblInfo, oldIDs); err != nil {
		return nil, nil, err
	}
	return oldDefinitions, newDefinitions, nil
}

func (w *worker) cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx *jobContext, job *model.Job, tblInfo *model.TableInfo, oldIDs []int64) (bool, error) {
	tbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, errors.Trace(err)
	}
	pt, ok := tbl.(table.PartitionedTable)
	if !ok {
		return false, dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	elements := make([]*meta.Element, 0, len(tblInfo.Indices))
	for _, idxInfo := range tblInfo.Indices {
		if idxInfo.Global {
			elements = append(elements, &meta.Element{ID: idxInfo.ID, TypeKey: meta.IndexElementKey})
		}
	}
	if len(elements) == 0 {
		return true, nil
	}
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		return false, err1
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	reorgInfo, err := getReorgInfoFromPartitions(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, pt, oldIDs, elements)

	if err != nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, errors.Trace(err)
	}
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (dropIndexErr error) {
		defer tidbutil.Recover(metrics.LabelDDL, "onDropTablePartition",
			func() {
				dropIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("drop partition panic")
			}, false)
		return w.cleanupGlobalIndexes(pt, oldIDs, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, nil
		}
		if dbterror.ErrPausedDDLJob.Equal(err) {
			// if ErrPausedDDLJob, we should return, check for the owner and re-wait job done.
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// onTruncateTablePartition truncates old partition meta.
//
// # StateNone
//
// Unaware of DDL.
//
// # StateWriteOnly
//
// Still sees and uses the old partition, but should filter out index reads of
// global index which has ids from pi.NewPartitionIDs.
// Allow duplicate key errors even if one cannot access the global index entry by reading!
// This state is not really needed if there are no global indexes, but used for consistency.
//
// # StateDeleteOnly
//
// Sees new partition, but should filter out index reads of global index which
// has ids from pi.DroppingDefinitions.
// Allow duplicate key errors even if one cannot access the global index entry by reading!
//
// # StateDeleteReorganization
//
// Now no other session has access to the old partition,
// but there are global index entries left pointing to the old partition,
// so they should be filtered out (see pi.DroppingDefinitions) and on write (insert/update)
// the old partition's row should be deleted and the global index key allowed
// to be overwritten.
// During this time the old partition is read and removing matching entries in
// smaller batches.
// This state is not really needed if there are no global indexes, but used for consistency.
//
// # StatePublic
//
// DDL done.
func (w *worker) onTruncateTablePartition(jobCtx *jobContext, job *model.Job) (int64, error) {
	var ver int64
	canCancel := false
	if job.SchemaState == model.StatePublic {
		canCancel = true
	}
	args, err := model.GetTruncateTableArgs(job)
	if err != nil {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	oldIDs, newIDs := args.OldPartitionIDs, args.NewPartitionIDs
	if len(oldIDs) != len(newIDs) {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(errors.New("len(oldIDs) must be the same as len(newIDs)"))
	}
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, job.SchemaID)
	if err != nil {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		if canCancel {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(dbterror.ErrPartitionMgmtOnNonpartitioned)
	}

	if job.IsRollingback() {
		return convertTruncateTablePartitionJob2RollbackJob(jobCtx, job, dbterror.ErrCancelledDDLJob, tblInfo)
	}

	failpoint.Inject("truncatePartCancel1", func(val failpoint.Value) {
		if val.(bool) {
			job.State = model.JobStateCancelled
			err = errors.New("Injected error by truncatePartCancel1")
			failpoint.Return(ver, err)
		}
	})

	var oldDefinitions []model.PartitionDefinition
	var newDefinitions []model.PartitionDefinition

	switch job.SchemaState {
	case model.StatePublic:
		// This work as a flag to ignore Global Index entries from the new partitions!
		// Used in IDsInDDLToIgnore() for filtering new partitions from
		// the global index
		pi.NewPartitionIDs = newIDs[:]
		pi.DDLAction = model.ActionTruncateTablePartition

		job.SchemaState = model.StateWriteOnly
		pi.DDLState = job.SchemaState
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// We can still rollback here, since we have not yet started to write to the new partitions!
		oldDefinitions, newDefinitions, err = replaceTruncatePartitions(job, jobCtx.metaMut, tblInfo, oldIDs, newIDs)
		if err != nil {
			return ver, errors.Trace(err)
		}
		var scatterScope string
		if val, ok := job.GetSystemVars(vardef.TiDBScatterRegion); ok {
			scatterScope = val
		}
		preSplitAndScatter(w.sess.Context, jobCtx.store, tblInfo, newDefinitions, scatterScope)
		failpoint.Inject("truncatePartFail1", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				err = errors.New("Injected error by truncatePartFail1")
				failpoint.Return(ver, err)
			}
		})
		// This work as a flag to ignore Global Index entries from the old partitions!
		// Used in IDsInDDLToIgnore() for filtering old partitions from
		// the global index
		pi.DroppingDefinitions = oldDefinitions
		// And we don't need to filter for new partitions any longer
		job.SchemaState = model.StateDeleteOnly
		pi.DDLState = job.SchemaState
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteOnly:
		// Now we don't see the old partitions, but other sessions may still use them.
		// So to keep the Global Index consistent, we will still keep it up-to-date with
		// the old partitions, as well as the new partitions.
		// Also ensures that no writes will happen after GC in DeleteRanges.

		job.SchemaState = model.StateDeleteReorganization
		pi.DDLState = job.SchemaState
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateDeleteReorganization:
		// Now the old partitions are no longer accessible, but they are still referenced in
		// the global indexes (although allowed to be overwritten).
		// So time to clear them.

		var done bool
		done, err = w.cleanGlobalIndexEntriesFromDroppedPartitions(jobCtx, job, tblInfo, oldIDs)
		if err != nil || !done {
			return ver, errors.Trace(err)
		}
		failpoint.Inject("truncatePartFail2", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				err = errors.New("Injected error by truncatePartFail2")
				failpoint.Return(ver, err)
			}
		})
		// For the truncatePartitionEvent
		oldDefinitions = pi.DroppingDefinitions
		newDefinitions = make([]model.PartitionDefinition, 0, len(oldIDs))
		for i, def := range oldDefinitions {
			newDef := def.Clone()
			newDef.ID = newIDs[i]
			newDefinitions = append(newDefinitions, newDef)
		}

		pi.DroppingDefinitions = nil
		pi.NewPartitionIDs = nil
		pi.DDLState = model.StateNone
		pi.DDLAction = model.ActionNone

		failpoint.Inject("truncatePartFail3", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				err = errors.New("Injected error by truncatePartFail3")
				failpoint.Return(ver, err)
			}
		})
		// used by ApplyDiff in updateSchemaVersion
		args.ShouldUpdateAffectedPartitions = true
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		truncatePartitionEvent := notifier.NewTruncatePartitionEvent(
			tblInfo,
			&model.PartitionInfo{Definitions: newDefinitions},
			&model.PartitionInfo{Definitions: oldDefinitions},
		)
		err = asyncNotifyEvent(jobCtx, truncatePartitionEvent, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(&model.TruncateTableArgs{
			OldPartitionIDs: oldIDs,
		})
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}
	return ver, errors.Trace(err)
}

func clearTruncatePartitionTiflashStatus(tblInfo *model.TableInfo, newPartitions []model.PartitionDefinition, oldIDs []int64) error {
	// Clear the tiflash replica available status.
	if tblInfo.TiFlashReplica != nil {
		e := infosync.ConfigureTiFlashPDForPartitions(true, &newPartitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID)
		failpoint.Inject("FailTiFlashTruncatePartition", func() {
			e = errors.New("enforced error")
		})
		if e != nil {
			logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(e))
			return e
		}
		tblInfo.TiFlashReplica.Available = false
		// Set partition replica become unavailable.
		removeTiFlashAvailablePartitionIDs(tblInfo, oldIDs)
	}
	return nil
}

func updateTruncatePartitionLabelRules(job *model.Job, t *meta.Mutator, oldPartitions, newPartitions []model.PartitionDefinition, tblInfo *model.TableInfo, oldIDs []int64) error {
	bundles, err := placement.NewPartitionListBundles(t, newPartitions)
	if err != nil {
		return errors.Trace(err)
	}

	tableBundle, err := placement.NewTableBundle(t, tblInfo)
	if err != nil {
		return errors.Trace(err)
	}

	if tableBundle != nil {
		bundles = append(bundles, tableBundle)
	}

	// create placement groups for each dropped partition to keep the data's placement before GC
	// These placements groups will be deleted after GC
	keepDroppedBundles, err := droppedPartitionBundles(t, tblInfo, oldPartitions)
	if err != nil {
		return errors.Trace(err)
	}
	bundles = append(bundles, keepDroppedBundles...)

	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		return errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	tableID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L)
	oldPartRules := make([]string, 0, len(oldIDs))
	for _, newPartition := range newPartitions {
		oldPartRuleID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, job.SchemaName, tblInfo.Name.L, newPartition.Name.L)
		oldPartRules = append(oldPartRules, oldPartRuleID)
	}

	rules, err := infosync.GetLabelRules(context.TODO(), append(oldPartRules, tableID))
	if err != nil {
		return errors.Wrapf(err, "failed to get label rules from PD")
	}

	newPartIDs := getPartitionIDs(tblInfo)
	newRules := make([]*label.Rule, 0, len(oldIDs)+1)
	if tr, ok := rules[tableID]; ok {
		newRules = append(newRules, tr.Clone().Reset(job.SchemaName, tblInfo.Name.L, "", append(newPartIDs, tblInfo.ID)...))
	}

	for idx, newPartition := range newPartitions {
		if pr, ok := rules[oldPartRules[idx]]; ok {
			newRules = append(newRules, pr.Clone().Reset(job.SchemaName, tblInfo.Name.L, newPartition.Name.L, newPartition.ID))
		}
	}

	patch := label.NewRulePatch(newRules, []string{})
	err = infosync.UpdateLabelRules(context.TODO(), patch)
	if err != nil {
		return errors.Wrapf(err, "failed to notify PD the label rules")
	}

	return nil
}

// onExchangeTablePartition exchange partition data
func (w *worker) onExchangeTablePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetExchangeTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	defID, ptSchemaID, ptID, partName :=
		args.PartitionID, args.PTSchemaID, args.PTTableID, args.PartitionName
	metaMut := jobCtx.metaMut

	ntDbInfo, err := checkSchemaExistAndCancelNotExistJob(metaMut, job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	ptDbInfo, err := metaMut.GetDatabase(ptSchemaID)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	nt, err := GetTableInfoAndCancelFaultJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if job.IsRollingback() {
		return rollbackExchangeTablePartition(jobCtx, job, nt)
	}
	pt, err := getTableInfo(metaMut, ptID, ptSchemaID)
	if err != nil {
		if infoschema.ErrDatabaseNotExists.Equal(err) || infoschema.ErrTableNotExists.Equal(err) {
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	_, partDef, err := getPartitionDef(pt, partName)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if job.SchemaState == model.StateNone {
		if pt.State != model.StatePublic {
			job.State = model.JobStateCancelled
			return ver, dbterror.ErrInvalidDDLState.GenWithStack("table %s is not in public, but %s", pt.Name, pt.State)
		}
		err = checkExchangePartition(pt, nt)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkTableDefCompatible(pt, nt)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkExchangePartitionPlacementPolicy(metaMut, nt.PlacementPolicyRef, pt.PlacementPolicyRef, partDef.PlacementPolicyRef)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		if defID != partDef.ID {
			logutil.DDLLogger().Info("Exchange partition id changed, updating to actual id",
				zap.Stringer("job", job), zap.Int64("defID", defID), zap.Int64("partDef.ID", partDef.ID))
			args.PartitionID = partDef.ID
			job.FillArgs(args)
			defID = partDef.ID
			err = updateDDLJob2Table(jobCtx.stepCtx, w.sess, job, true)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}
		var ptInfo []schemaIDAndTableInfo
		if len(nt.Constraints) > 0 {
			pt.ExchangePartitionInfo = &model.ExchangePartitionInfo{
				ExchangePartitionTableID: nt.ID,
				ExchangePartitionDefID:   defID,
			}
			ptInfo = append(ptInfo, schemaIDAndTableInfo{
				schemaID: ptSchemaID,
				tblInfo:  pt,
			})
		}
		nt.ExchangePartitionInfo = &model.ExchangePartitionInfo{
			ExchangePartitionTableID: ptID,
			ExchangePartitionDefID:   defID,
		}
		// We need an interim schema version,
		// so there are no non-matching rows inserted
		// into the table using the schema version
		// before the exchange is made.
		job.SchemaState = model.StateWriteOnly
		pt.Partition.DDLState = job.SchemaState
		pt.Partition.DDLAction = job.Type
		return updateVersionAndTableInfoWithCheck(jobCtx, job, nt, true, ptInfo...)
	}
	// From now on, nt (the non-partitioned table) has
	// ExchangePartitionInfo set, meaning it is restricted
	// to only allow writes that would match the
	// partition to be exchange with.
	// So we need to rollback that change, instead of just cancelling.

	delayForAsyncCommit()

	if defID != partDef.ID {
		// Should never happen, should have been updated above, in previous state!
		logutil.DDLLogger().Error("Exchange partition id changed, updating to actual id",
			zap.Stringer("job", job), zap.Int64("defID", defID), zap.Int64("partDef.ID", partDef.ID))
		args.PartitionID = partDef.ID
		job.FillArgs(args)
		// might be used later, ignore the lint warning.
		//nolint: ineffassign
		defID = partDef.ID
		err = updateDDLJob2Table(jobCtx.stepCtx, w.sess, job, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
	}

	if args.WithValidation {
		ntbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, nt)
		if err != nil {
			return ver, errors.Trace(err)
		}
		ptbl, err := getTable(jobCtx.getAutoIDRequirement(), ptSchemaID, pt)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = checkExchangePartitionRecordValidation(
			jobCtx.stepCtx,
			w,
			ptbl,
			ntbl,
			ptDbInfo.Name.L,
			ntDbInfo.Name.L,
			partName,
		)
		if err != nil {
			job.State = model.JobStateRollingback
			return ver, errors.Trace(err)
		}
	}

	// partition table auto IDs.
	ptAutoIDs, err := metaMut.GetAutoIDAccessors(ptSchemaID, ptID).Get()
	if err != nil {
		return ver, errors.Trace(err)
	}
	// non-partition table auto IDs.
	ntAutoIDs, err := metaMut.GetAutoIDAccessors(job.SchemaID, nt.ID).Get()
	if err != nil {
		return ver, errors.Trace(err)
	}

	if pt.TiFlashReplica != nil {
		for i, id := range pt.TiFlashReplica.AvailablePartitionIDs {
			if id == partDef.ID {
				pt.TiFlashReplica.AvailablePartitionIDs[i] = nt.ID
				break
			}
		}
	}

	// Recreate non-partition table meta info,
	// by first delete it with the old table id
	err = metaMut.DropTableOrView(job.SchemaID, nt.ID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// exchange table meta id
	pt.ExchangePartitionInfo = nil
	// Used below to update the partitioned table's stats meta.
	originalPartitionDef := partDef.Clone()
	originalNt := nt.Clone()
	partDef.ID, nt.ID = nt.ID, partDef.ID
	pt.Partition.DDLState = model.StateNone
	pt.Partition.DDLAction = model.ActionNone

	err = metaMut.UpdateTable(ptSchemaID, pt)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = metaMut.CreateTableOrView(job.SchemaID, nt)
	if err != nil {
		return ver, errors.Trace(err)
	}

	failpoint.Inject("exchangePartitionErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(ver, errors.New("occur an error after updating partition id"))
		}
	})

	// Set both tables to the maximum auto IDs between normal table and partitioned table.
	// TODO: Fix the issue of big transactions during EXCHANGE PARTITION with AutoID.
	// Similar to https://github.com/pingcap/tidb/issues/46904
	newAutoIDs := model.AutoIDGroup{
		RowID:       max(ptAutoIDs.RowID, ntAutoIDs.RowID),
		IncrementID: max(ptAutoIDs.IncrementID, ntAutoIDs.IncrementID),
		RandomID:    max(ptAutoIDs.RandomID, ntAutoIDs.RandomID),
	}
	err = metaMut.GetAutoIDAccessors(ptSchemaID, pt.ID).Put(newAutoIDs)
	if err != nil {
		return ver, errors.Trace(err)
	}
	err = metaMut.GetAutoIDAccessors(job.SchemaID, nt.ID).Put(newAutoIDs)
	if err != nil {
		return ver, errors.Trace(err)
	}

	failpoint.Inject("exchangePartitionAutoID", func(val failpoint.Value) {
		if val.(bool) {
			seCtx, err := w.sessPool.Get()
			defer w.sessPool.Put(seCtx)
			if err != nil {
				failpoint.Return(ver, err)
			}
			se := sess.NewSession(seCtx)
			_, err = se.Execute(context.Background(), "insert ignore into test.pt values (40000000)", "exchange_partition_test")
			if err != nil {
				failpoint.Return(ver, err)
			}
		}
	})

	// the follow code is a swap function for rules of two partitions
	// though partitions has exchanged their ID, swap still take effect

	bundles, err := bundlesForExchangeTablePartition(metaMut, pt, partDef, nt)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles); err != nil {
		return ver, errors.Wrapf(err, "failed to notify PD the placement rules")
	}

	ntrID := fmt.Sprintf(label.TableIDFormat, label.IDPrefix, job.SchemaName, nt.Name.L)
	ptrID := fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, job.SchemaName, pt.Name.L, partDef.Name.L)

	rules, err := infosync.GetLabelRules(context.TODO(), []string{ntrID, ptrID})
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get PD the label rules")
	}

	ntr := rules[ntrID]
	ptr := rules[ptrID]

	// This must be a bug, nt cannot be partitioned!
	partIDs := getPartitionIDs(nt)

	var setRules []*label.Rule
	var deleteRules []string
	if ntr != nil && ptr != nil {
		setRules = append(setRules, ntr.Clone().Reset(job.SchemaName, pt.Name.L, partDef.Name.L, partDef.ID))
		setRules = append(setRules, ptr.Clone().Reset(job.SchemaName, nt.Name.L, "", append(partIDs, nt.ID)...))
	} else if ptr != nil {
		setRules = append(setRules, ptr.Clone().Reset(job.SchemaName, nt.Name.L, "", append(partIDs, nt.ID)...))
		// delete ptr
		deleteRules = append(deleteRules, ptrID)
	} else if ntr != nil {
		setRules = append(setRules, ntr.Clone().Reset(job.SchemaName, pt.Name.L, partDef.Name.L, partDef.ID))
		// delete ntr
		deleteRules = append(deleteRules, ntrID)
	}

	patch := label.NewRulePatch(setRules, deleteRules)
	err = infosync.UpdateLabelRules(context.TODO(), patch)
	if err != nil {
		return ver, errors.Wrapf(err, "failed to notify PD the label rules")
	}

	job.SchemaState = model.StatePublic
	nt.ExchangePartitionInfo = nil
	ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, nt, true)
	if err != nil {
		return ver, errors.Trace(err)
	}
	exchangePartitionEvent := notifier.NewExchangePartitionEvent(
		pt,
		&model.PartitionInfo{Definitions: []model.PartitionDefinition{originalPartitionDef}},
		originalNt,
	)
	err = asyncNotifyEvent(jobCtx, exchangePartitionEvent, job, noSubJob, w.sess)
	if err != nil {
		return ver, errors.Trace(err)
	}

	job.FinishTableJob(model.JobStateDone, model.StateNone, ver, pt)
	return ver, nil
}

func getNewGlobal(partInfo *model.PartitionInfo, idx *model.IndexInfo) bool {
	for _, newIdx := range partInfo.DDLUpdateIndexes {
		if strings.EqualFold(idx.Name.L, newIdx.IndexName) {
			return newIdx.Global
		}
	}
	return idx.Global
}

func getReorgPartitionInfo(t *meta.Mutator, job *model.Job, args *model.TablePartitionArgs) (*model.TableInfo, []string, *model.PartitionInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	partNames, partInfo := args.PartNames, args.PartInfo
	if job.SchemaState == model.StateNone {
		if tblInfo.Partition != nil {
			tblInfo.Partition.NewTableID = partInfo.NewTableID
			tblInfo.Partition.DDLType = partInfo.Type
			tblInfo.Partition.DDLExpr = partInfo.Expr
			tblInfo.Partition.DDLColumns = partInfo.Columns
		} else {
			tblInfo.Partition = getPartitionInfoTypeNone()
			tblInfo.Partition.NewTableID = partInfo.NewTableID
			tblInfo.Partition.Definitions[0].ID = tblInfo.ID
			tblInfo.Partition.DDLType = partInfo.Type
			tblInfo.Partition.DDLExpr = partInfo.Expr
			tblInfo.Partition.DDLColumns = partInfo.Columns
		}
	}
	return tblInfo, partNames, partInfo, nil
}

// onReorganizePartition reorganized the partitioning of a table including its indexes.
// ALTER TABLE t REORGANIZE PARTITION p0 [, p1...] INTO (PARTITION p0 ...)
//
//	Takes one set of partitions and copies the data to a newly defined set of partitions
//
// ALTER TABLE t REMOVE PARTITIONING
//
//	Makes a partitioned table non-partitioned, by first collapsing all partitions into a
//	single partition and then converts that partition to a non-partitioned table
//
// ALTER TABLE t PARTITION BY ...
//
//	Changes the partitioning to the newly defined partitioning type and definitions,
//	works for both partitioned and non-partitioned tables.
//	If the table is non-partitioned, then it will first convert it to a partitioned
//	table with a single partition, i.e. the full table as a single partition.
//
// job.SchemaState goes through the following SchemaState(s):
// StateNone -> StateDeleteOnly -> StateWriteOnly -> StateWriteReorganization
// -> StateDeleteOrganization -> StatePublic -> Done
// There are more details embedded in the implementation, but the high level changes are:
//
// StateNone -> StateDeleteOnly:
//
//	Various checks and validations.
//	Add possible new unique/global indexes. They share the same state as job.SchemaState
//	until end of StateWriteReorganization -> StateDeleteReorganization.
//	Set DroppingDefinitions and AddingDefinitions.
//	So both the new partitions and new indexes will be included in new delete/update DML.
//
// StateDeleteOnly -> StateWriteOnly:
//
//	So both the new partitions and new indexes will be included also in update/insert DML.
//
// StateWriteOnly -> StateWriteReorganization:
//
//	To make sure that when we are reorganizing the data,
//	both the old and new partitions/indexes will be updated.
//
// StateWriteReorganization -> StateDeleteOrganization:
//
//	Here is where all data is reorganized, both partitions and indexes.
//	It copies all data from the old set of partitions into the new set of partitions,
//	and creates the local indexes on the new set of partitions,
//	and if new unique indexes are added, it also updates them with the rest of data from
//	the non-touched partitions.
//	For indexes that are to be replaced with new ones (old/new global index),
//	mark the old indexes as StateWriteOnly and new ones as StatePublic
//	Finally make the table visible with the new partition definitions.
//	I.e. in this state clients will read from the old set of partitions,
//	and next state will read the new set of partitions in StateDeleteReorganization.
//
// StateDeleteOrganization -> StatePublic:
//
//	Now we mark all replaced (old) indexes as StateDeleteOnly
//	in case DeleteRange would be called directly after the DDL,
//	this way there will be no orphan records inserted after DeleteRanges
//	has cleaned up the old partitions and old global indexes.
//
// StatePublic -> Done:
//
//	Now all heavy lifting is done, and we just need to finalize and drop things, while still doing
//	double writes, since previous state sees the old partitions/indexes.
//	Remove the old indexes and old partitions from the TableInfo.
//	Add the old indexes and old partitions to the queue for later cleanup (see delete_range.go).
//	Queue new partitions for statistics update.
//	if ALTER TABLE t PARTITION BY/REMOVE PARTITIONING:
//	  Recreate the table with the new TableID, by DropTableOrView+CreateTableOrView
//
// Done:
//
//	Everything now looks as it should, no memory of old partitions/indexes,
//	and no more double writing, since the previous state is only using the new partitions/indexes.
//
// Note: Special handling is also required in tables.newPartitionedTable(),
// to get per partition indexes in the right state.
func (w *worker) onReorganizePartition(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTablePartitionArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	jobCtx.jobArgs = args
	// Handle the rolling back job
	if job.IsRollingback() {
		return w.rollbackLikeDropPartition(jobCtx, job)
	}

	tblInfo, partNames, partInfo, err := getReorgPartitionInfo(jobCtx.metaMut, job, args)
	if err != nil {
		return ver, err
	}

	metaMut := jobCtx.metaMut
	switch job.SchemaState {
	case model.StateNone:
		// job.SchemaState == model.StateNone means the job is in the initial state of reorg partition.
		// Here should use partInfo from job directly and do some check action.
		// In case there was a race for queueing different schema changes on the same
		// table and the checks was not done on the current schema version.
		// The partInfo may have been checked against an older schema version for example.
		// If the check is done here, it does not need to be repeated, since no other
		// DDL on the same table can be run concurrently.
		tblInfo.Partition.DDLAction = job.Type
		num := len(partInfo.Definitions) - len(partNames) + len(tblInfo.Partition.Definitions)
		err = checkAddPartitionTooManyPartitions(uint64(num))
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		err = checkReorgPartitionNames(tblInfo.Partition, partNames, partInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		// Re-check that the dropped/added partitions are compatible with current definition
		firstPartIdx, lastPartIdx, idMap, err := getReplacedPartitionIDs(partNames, tblInfo.Partition)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		sctx := w.sess.Context
		if err = checkReorgPartitionDefs(sctx, job.Type, tblInfo, partInfo, firstPartIdx, lastPartIdx, idMap); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}

		if job.Type == model.ActionAlterTablePartitioning {
			// Also verify same things as in CREATE TABLE ... PARTITION BY
			if len(partInfo.Columns) > 0 {
				// shallow copy, only for reading/checking
				tmpTblInfo := *tblInfo
				tmpTblInfo.Partition = partInfo
				if err = checkColumnsPartitionType(&tmpTblInfo); err != nil {
					job.State = model.JobStateCancelled
					return ver, err
				}
			} else {
				if err = checkPartitionFuncType(sctx.GetExprCtx(), partInfo.Expr, job.SchemaName, tblInfo); err != nil {
					job.State = model.JobStateCancelled
					return ver, err
				}
			}
		}
		// move the adding definition into tableInfo.
		updateAddingPartitionInfo(partInfo, tblInfo)
		orgDefs := tblInfo.Partition.Definitions
		updateDroppingPartitionInfo(tblInfo, partNames)
		// Reset original partitions, and keep DroppedDefinitions
		tblInfo.Partition.Definitions = orgDefs

		// modify placement settings
		for _, def := range tblInfo.Partition.AddingDefinitions {
			if _, err = checkPlacementPolicyRefValidAndCanNonValidJob(metaMut, job, def.PlacementPolicyRef); err != nil {
				// job.State = model.JobStateCancelled may be set depending on error in function above.
				return ver, errors.Trace(err)
			}
		}

		// All global indexes must be recreated, we cannot update them in-place, since we must have
		// both old and new set of partition ids in the unique index at the same time!
		// We also need to recreate and change between non-global unique indexes and global index,
		// in case a new PARTITION BY changes if all partition columns are included or not.
		for _, index := range tblInfo.Indices {
			newGlobal := getNewGlobal(partInfo, index)
			if job.Type == model.ActionRemovePartitioning {
				// When removing partitioning, set all indexes to 'local' since it will become a non-partitioned table!
				newGlobal = false
			}
			if !index.Global && !newGlobal {
				continue
			}
			inAllPartitionColumns, err := checkPartitionKeysConstraint(partInfo, index.Columns, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
			// Currently only support Explicit Global indexes for unique index.
			if !inAllPartitionColumns && !newGlobal && index.Unique {
				job.State = model.JobStateCancelled
				return ver, dbterror.ErrGlobalIndexNotExplicitlySet.GenWithStackByArgs(index.Name.O)
			}
			if tblInfo.Partition.DDLChangedIndex == nil {
				tblInfo.Partition.DDLChangedIndex = make(map[int64]bool)
			}
			// Duplicate the unique indexes with new index ids.
			// If previously was Global or will be Global:
			// it must be recreated with new index ID
			// TODO: Could we allow that session in StateWriteReorganization, when StateDeleteReorganization
			// has started, may not find changes through the global index that sessions in StateDeleteReorganization made?
			// If so, then we could avoid copying the full Global Index if it has not changed from LOCAL!
			// It might be possible to use the new, not yet public partitions to access those rows?!
			// Just that it would not work with explicit partition select SELECT FROM t PARTITION (p,...)
			newIndex := index.Clone()
			newIndex.State = model.StateDeleteOnly
			newIndex.ID = AllocateIndexID(tblInfo)
			tblInfo.Partition.DDLChangedIndex[index.ID] = false
			tblInfo.Partition.DDLChangedIndex[newIndex.ID] = true
			newIndex.Global = newGlobal
			tblInfo.Indices = append(tblInfo.Indices, newIndex)
		}
		failpoint.Inject("reorgPartCancel1", func(val failpoint.Value) {
			if val.(bool) {
				job.State = model.JobStateCancelled
				failpoint.Return(ver, errors.New("Injected error by reorgPartCancel1"))
			}
		})
		// From now on we cannot just cancel the DDL, we must roll back if changesMade!
		changesMade := false
		if tblInfo.TiFlashReplica != nil {
			// Must set placement rule, and make sure it succeeds.
			if err := infosync.ConfigureTiFlashPDForPartitions(true, &tblInfo.Partition.AddingDefinitions, tblInfo.TiFlashReplica.Count, &tblInfo.TiFlashReplica.LocationLabels, tblInfo.ID); err != nil {
				logutil.DDLLogger().Error("ConfigureTiFlashPDForPartitions fails", zap.Error(err))
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			changesMade = true
			// In the next step, StateDeleteOnly, wait to verify the TiFlash replicas are OK
		}

		changed, err := alterTablePartitionBundles(metaMut, tblInfo, tblInfo.Partition.AddingDefinitions)
		if err != nil {
			if !changesMade {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			return rollbackReorganizePartitionWithErr(jobCtx, job, err)
		}
		changesMade = changesMade || changed

		ids := getIDs([]*model.TableInfo{tblInfo})
		for _, p := range tblInfo.Partition.AddingDefinitions {
			ids = append(ids, p.ID)
		}
		changed, err = alterTableLabelRule(job.SchemaName, tblInfo, ids)
		changesMade = changesMade || changed
		if err != nil {
			if !changesMade {
				job.State = model.JobStateCancelled
				return ver, err
			}
			return rollbackReorganizePartitionWithErr(jobCtx, job, err)
		}

		// Doing the preSplitAndScatter here, since all checks are completed,
		// and we will soon start writing to the new partitions.
		if s, ok := jobCtx.store.(kv.SplittableStore); ok && s != nil {
			// 1. partInfo only contains the AddingPartitions
			// 2. ScatterTable control all new split region need waiting for scatter region finish at table level.
			splitPartitionTableRegion(w.sess.Context, s, tblInfo, partInfo.Definitions, vardef.ScatterTable)
		}

		if job.Type == model.ActionReorganizePartition {
			tblInfo.Partition.SetOriginalPartitionIDs()
		}

		// Assume we cannot have more than MaxUint64 rows, set the progress to 1/10 of that.
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String(), "").Set(0.1 / float64(math.MaxUint64))
		job.SchemaState = model.StateDeleteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		failpoint.Inject("reorgPartRollback1", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback1")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})

		// Is really both StateDeleteOnly AND StateWriteOnly needed?
		// If transaction A in WriteOnly inserts row 1 (into both new and old partition set)
		// and then transaction B in DeleteOnly deletes that row (in both new and old)
		// does really transaction B need to do the delete in the new partition?
		// Yes, otherwise it would still be there when the WriteReorg happens,
		// and WriteReorg would only copy existing rows to the new table, so unless it is
		// deleted it would result in a ghost row!
		// What about update then?
		// Updates also need to be handled for new partitions in DeleteOnly,
		// since it would not be overwritten during Reorganize phase.
		// BUT if the update results in adding in one partition and deleting in another,
		// THEN only the delete must happen in the new partition set, not the insert!
	case model.StateDeleteOnly:
		// This state is to confirm all servers can not see the new partitions when reorg is running,
		// so that all deletes will be done in both old and new partitions when in either DeleteOnly
		// or WriteOnly state.
		// Also using the state for checking that the optional TiFlash replica is available, making it
		// in a state without (much) data and easy to retry without side effects.

		// Reason for having it here, is to make it easy for retry, and better to make sure it is in-sync
		// as early as possible, to avoid a long wait after the data copying.
		if tblInfo.TiFlashReplica != nil && tblInfo.TiFlashReplica.Available {
			// For available state, the new added partition should wait its replica to
			// be finished, otherwise the query to this partition will be blocked.
			count := tblInfo.TiFlashReplica.Count
			needRetry, err := checkPartitionReplica(count, tblInfo.Partition.AddingDefinitions, jobCtx)
			if err != nil {
				return rollbackReorganizePartitionWithErr(jobCtx, job, err)
			}
			if needRetry {
				// The new added partition hasn't been replicated.
				// Do nothing to the job this time, wait next worker round.
				time.Sleep(tiflashCheckTiDBHTTPAPIHalfInterval)
				// Set the error here which will lead this job exit when it's retry times beyond the limitation.
				return ver, errors.Errorf("[ddl] add partition wait for tiflash replica to complete")
			}

			// When TiFlash Replica is ready, we must move them into `AvailablePartitionIDs`.
			// Since onUpdateFlashReplicaStatus cannot see the partitions yet (not public)
			for _, d := range tblInfo.Partition.AddingDefinitions {
				tblInfo.TiFlashReplica.AvailablePartitionIDs = append(tblInfo.TiFlashReplica.AvailablePartitionIDs, d.ID)
			}
		}

		for i := range tblInfo.Indices {
			if tblInfo.Indices[i].State == model.StateDeleteOnly {
				tblInfo.Indices[i].State = model.StateWriteOnly
			}
		}
		tblInfo.Partition.DDLState = model.StateWriteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String(), "").Set(0.2 / float64(math.MaxUint64))
		failpoint.Inject("reorgPartRollback2", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback2")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})
		job.SchemaState = model.StateWriteOnly
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteOnly:
		// Insert this state to confirm all servers can see the new partitions when reorg is running,
		// so that new data will be updated in both old and new partitions when reorganizing.
		job.SnapshotVer = 0
		for i := range tblInfo.Indices {
			if tblInfo.Indices[i].State == model.StateWriteOnly {
				tblInfo.Indices[i].State = model.StateWriteReorganization
			}
		}
		job.SchemaState = model.StateWriteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		metrics.GetBackfillProgressByLabel(metrics.LblReorgPartition, job.SchemaName, tblInfo.Name.String(), "").Set(0.3 / float64(math.MaxUint64))
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
	case model.StateWriteReorganization:
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		tbl, err2 := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, tblInfo)
		if err2 != nil {
			return ver, errors.Trace(err2)
		}
		failpoint.Inject("reorgPartFail1", func(val failpoint.Value) {
			// Failures will retry, then do rollback
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail1"))
			}
		})
		failpoint.Inject("reorgPartRollback3", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback3")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})
		var done bool
		done, ver, err = doPartitionReorgWork(w, jobCtx, job, tbl, physicalTableIDs)

		if !done {
			return ver, err
		}

		failpoint.Inject("reorgPartRollback4", func(val failpoint.Value) {
			if val.(bool) {
				err = errors.New("Injected error by reorgPartRollback4")
				failpoint.Return(rollbackReorganizePartitionWithErr(jobCtx, job, err))
			}
		})

		for _, index := range tblInfo.Indices {
			isNew, ok := tblInfo.Partition.DDLChangedIndex[index.ID]
			if !ok {
				continue
			}
			if isNew {
				// Newly created index, replacing old unique/global index
				index.State = model.StatePublic
				continue
			}
			// Old index, should not be visible any longer,
			// but needs to be kept up-to-date in case rollback happens.
			index.State = model.StateWriteOnly
		}
		firstPartIdx, lastPartIdx, idMap, err2 := getReplacedPartitionIDs(partNames, tblInfo.Partition)
		if err2 != nil {
			return ver, err2
		}
		newDefs := getReorganizedDefinitions(tblInfo.Partition, firstPartIdx, lastPartIdx, idMap)

		// From now on, use the new partitioning, but keep the Adding and Dropping for double write
		tblInfo.Partition.Definitions = newDefs
		tblInfo.Partition.Num = uint64(len(newDefs))
		if job.Type == model.ActionAlterTablePartitioning ||
			job.Type == model.ActionRemovePartitioning {
			tblInfo.Partition.Type, tblInfo.Partition.DDLType = tblInfo.Partition.DDLType, tblInfo.Partition.Type
			tblInfo.Partition.Expr, tblInfo.Partition.DDLExpr = tblInfo.Partition.DDLExpr, tblInfo.Partition.Expr
			tblInfo.Partition.Columns, tblInfo.Partition.DDLColumns = tblInfo.Partition.DDLColumns, tblInfo.Partition.Columns
		}

		failpoint.Inject("reorgPartFail2", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail2"))
			}
		})

		// Now all the data copying is done, but we cannot simply remove the droppingDefinitions
		// since they are a part of the normal Definitions that other nodes with
		// the current schema version. So we need to double write for one more schema version
		job.SchemaState = model.StateDeleteReorganization
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)

	case model.StateDeleteReorganization:
		// Need to have one more state before completing, due to:
		// - DeleteRanges could possibly start directly after DDL causing
		//   inserts during previous state (DeleteReorg) could insert after the cleanup
		//   leaving data in dropped partitions/indexes that will not be cleaned up again.
		// - Updates in previous state (DeleteReorg) could have duplicate errors, if the row
		//   was deleted or updated in after finish (so here we need to have DeleteOnly index state!
		// And we cannot rollback in this state!

		// Stop double writing to the indexes, only do Deletes!
		// so that previous could do inserts, we do delete and allow second insert for
		// previous state clients!
		for _, index := range tblInfo.Indices {
			isNew, ok := tblInfo.Partition.DDLChangedIndex[index.ID]
			if !ok || isNew {
				continue
			}
			// Old index, should not be visible any longer,
			// but needs to be deleted, in case previous state clients inserts.
			index.State = model.StateDeleteOnly
		}
		failpoint.Inject("reorgPartFail3", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail3"))
			}
		})
		job.SchemaState = model.StatePublic
		tblInfo.Partition.DDLState = job.SchemaState
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)

	case model.StatePublic:
		// Drop the droppingDefinitions and finish the DDL
		// This state is needed for the case where client A sees the schema
		// with version of StateWriteReorg and would not see updates of
		// client B that writes to the new partitions, previously
		// addingDefinitions, since it would not double write to
		// the droppingDefinitions during this time
		// By adding StateDeleteReorg state, client B will write to both
		// the new (previously addingDefinitions) AND droppingDefinitions

		// Register the droppingDefinitions ids for rangeDelete
		// and the addingDefinitions for handling in the updateSchemaVersion
		physicalTableIDs := getPartitionIDsFromDefinitions(tblInfo.Partition.DroppingDefinitions)
		newIDs := getPartitionIDsFromDefinitions(partInfo.Definitions)
		statisticsPartInfo := &model.PartitionInfo{Definitions: tblInfo.Partition.AddingDefinitions}
		droppedPartInfo := &model.PartitionInfo{Definitions: tblInfo.Partition.DroppingDefinitions}

		tblInfo.Partition.DroppingDefinitions = nil
		tblInfo.Partition.AddingDefinitions = nil
		tblInfo.Partition.DDLState = model.StateNone
		tblInfo.Partition.DDLAction = model.ActionNone
		tblInfo.Partition.OriginalPartitionIDsOrder = nil

		var dropIndices []*model.IndexInfo
		for _, indexInfo := range tblInfo.Indices {
			if indexInfo.State == model.StateDeleteOnly {
				// Drop the old indexes, see onDropIndex
				indexInfo.State = model.StateNone
				DropIndexColumnFlag(tblInfo, indexInfo)
				RemoveDependentHiddenColumns(tblInfo, indexInfo)
				dropIndices = append(dropIndices, indexInfo)
			}
		}
		// Local indexes is not an issue, since they will be gone with the dropped
		// partitions, but replaced global indexes should be checked!
		for _, indexInfo := range dropIndices {
			removeIndexInfo(tblInfo, indexInfo)
			if indexInfo.Global {
				args.OldGlobalIndexes = append(args.OldGlobalIndexes, model.TableIDIndexID{TableID: tblInfo.ID, IndexID: indexInfo.ID})
			}
		}
		failpoint.Inject("reorgPartFail4", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail4"))
			}
		})
		var oldTblID int64
		if job.Type != model.ActionReorganizePartition {
			// ALTER TABLE ... PARTITION BY
			// REMOVE PARTITIONING
			// Storing the old table ID, used for updating statistics.
			oldTblID = tblInfo.ID
			// TODO: Add concurrent test!
			// TODO: Will this result in big gaps?
			// TODO: How to carrie over AUTO_INCREMENT etc.?
			// Check if they are carried over in ApplyDiff?!?
			autoIDs, err := metaMut.GetAutoIDAccessors(job.SchemaID, tblInfo.ID).Get()
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = metaMut.DropTableOrView(job.SchemaID, tblInfo.ID)
			if err != nil {
				return ver, errors.Trace(err)
			}
			tblInfo.ID = partInfo.NewTableID
			if oldTblID != physicalTableIDs[0] {
				// if partitioned before, then also add the old table ID,
				// otherwise it will be the already included first partition
				physicalTableIDs = append(physicalTableIDs, oldTblID)
			}
			if job.Type == model.ActionRemovePartitioning {
				tblInfo.Partition = nil
			} else {
				// ALTER TABLE ... PARTITION BY
				tblInfo.Partition.ClearReorgIntermediateInfo()
			}
			err = metaMut.GetAutoIDAccessors(job.SchemaID, tblInfo.ID).Put(autoIDs)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = metaMut.CreateTableOrView(job.SchemaID, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}
		}

		// We need to update the Placement rule bundles with the final partitions.
		_, err = alterTablePartitionBundles(metaMut, tblInfo, nil)
		if err != nil {
			return ver, err
		}

		failpoint.Inject("reorgPartFail5", func(val failpoint.Value) {
			if val.(bool) {
				job.ErrorCount += vardef.GetDDLErrorCountLimit() / 2
				failpoint.Return(ver, errors.New("Injected error by reorgPartFail5"))
			}
		})
		failpoint.Inject("updateVersionAndTableInfoErrInStateDeleteReorganization", func() {
			failpoint.Return(ver, errors.New("Injected error in StateDeleteReorganization"))
		})
		args.OldPhysicalTblIDs = physicalTableIDs
		args.NewPartitionIDs = newIDs
		job.SchemaState = model.StateNone
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// How to handle this?
		// Seems to only trigger asynchronous update of statistics.
		// Should it actually be synchronous?
		// Include the old table ID, if changed, which may contain global statistics,
		// so it can be reused for the new (non)partitioned table.
		event, err := newStatsDDLEventForJob(job.Type, oldTblID, tblInfo, statisticsPartInfo, droppedPartInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = asyncNotifyEvent(jobCtx, event, job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		// A background job will be created to delete old partition data.
		job.FillFinishedArgs(args)

	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("partition", job.SchemaState)
	}

	return ver, errors.Trace(err)
}

// newStatsDDLEventForJob creates a util.SchemaChangeEvent for a job.
// It is used for reorganize partition, add partitioning and remove partitioning.
func newStatsDDLEventForJob(
	jobType model.ActionType,
	oldTblID int64,
	tblInfo *model.TableInfo,
	addedPartInfo *model.PartitionInfo,
	droppedPartInfo *model.PartitionInfo,
) (*notifier.SchemaChangeEvent, error) {
	var event *notifier.SchemaChangeEvent
	switch jobType {
	case model.ActionReorganizePartition:
		event = notifier.NewReorganizePartitionEvent(
			tblInfo,
			addedPartInfo,
			droppedPartInfo,
		)
	case model.ActionAlterTablePartitioning:
		event = notifier.NewAddPartitioningEvent(
			oldTblID,
			tblInfo,
			addedPartInfo,
		)
	case model.ActionRemovePartitioning:
		event = notifier.NewRemovePartitioningEvent(
			oldTblID,
			tblInfo,
			droppedPartInfo,
		)
	default:
		return nil, errors.Errorf("unknown job type: %s", jobType.String())
	}
	return event, nil
}

func doPartitionReorgWork(w *worker, jobCtx *jobContext, job *model.Job, tbl table.Table, physTblIDs []int64) (done bool, ver int64, err error) {
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		return done, ver, err1
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	reorgTblInfo := tbl.Meta().Clone()
	var elements []*meta.Element
	indices := make([]*model.IndexInfo, 0, len(tbl.Meta().Indices))
	for _, index := range tbl.Meta().Indices {
		if isNew, ok := tbl.Meta().GetPartitionInfo().DDLChangedIndex[index.ID]; ok && !isNew {
			// Skip old replaced indexes, but rebuild all other indexes
			continue
		}
		indices = append(indices, index)
	}
	elements = BuildElements(tbl.Meta().Columns[0], indices)
	reorgTbl, err := getTable(jobCtx.getAutoIDRequirement(), job.SchemaID, reorgTblInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	partTbl, ok := reorgTbl.(table.PartitionedTable)
	if !ok {
		return false, ver, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfoFromPartitions(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, partTbl, physTblIDs, elements)
	err = w.runReorgJob(jobCtx, reorgInfo, reorgTbl.Meta(), func() (reorgErr error) {
		defer tidbutil.Recover(metrics.LabelDDL, "doPartitionReorgWork",
			func() {
				reorgErr = dbterror.ErrCancelledDDLJob.GenWithStack("reorganize partition for table `%v` panic", tbl.Meta().Name)
			}, false)
		return w.reorgPartitionDataAndIndex(jobCtx, reorgTbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}

		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// If timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.IsTxnRetryableError(err) {
			return false, ver, errors.Trace(err)
		}
		if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
			logutil.DDLLogger().Warn("reorg partition job failed, RemoveDDLReorgHandle failed, can't convert job to rollback",
				zap.Stringer("job", job), zap.Error(err1))
		}
		logutil.DDLLogger().Warn("reorg partition job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
		// TODO: Test and verify that this returns an error on the ALTER TABLE session.
		ver, err = rollbackReorganizePartitionWithErr(jobCtx, job, err)
		return false, ver, errors.Trace(err)
	}
	return true, ver, err
}

type reorgPartitionWorker struct {
	*backfillCtx
	// Static allocated to limit memory allocations
	rowRecords        []*rowRecord
	rowDecoder        *decoder.RowDecoder
	rowMap            map[int64]types.Datum
	writeColOffsetMap map[int64]int
	maxOffset         int
	reorgedTbl        table.PartitionedTable
}

func newReorgPartitionWorker(i int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*reorgPartitionWorker, error) {
	bCtx, err := newBackfillCtx(i, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblReorgPartitionRate, false)
	if err != nil {
		return nil, err
	}
	reorgedTbl, err := tables.GetReorganizedPartitionedTable(t)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pt := t.GetPartitionedTable()
	if pt == nil {
		return nil, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
	}
	partColIDs := reorgedTbl.GetPartitionColumnIDs()
	writeColOffsetMap := make(map[int64]int, len(partColIDs))
	maxOffset := 0
	for _, id := range partColIDs {
		var offset int
		for _, col := range pt.Cols() {
			if col.ID == id {
				offset = col.Offset
				break
			}
		}
		writeColOffsetMap[id] = offset
		maxOffset = max(maxOffset, offset)
	}
	return &reorgPartitionWorker{
		backfillCtx:       bCtx,
		rowDecoder:        decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap),
		rowMap:            make(map[int64]types.Datum, len(decodeColMap)),
		writeColOffsetMap: writeColOffsetMap,
		maxOffset:         maxOffset,
		reorgedTbl:        reorgedTbl,
	}, nil
}

func (w *reorgPartitionWorker) BackfillData(_ context.Context, handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

		nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		failpoint.InjectCall("PartitionBackfillData", len(w.rowRecords) > 0)
		// For non-clustered tables, we need to replace the _tidb_rowid handles since
		// there may be duplicates across different partitions, due to EXCHANGE PARTITION.
		// Meaning we need to check here if a record was double written to the new partition,
		// i.e. concurrently written by StateWriteOnly or StateWriteReorganization.
		// and if so, skip it.
		var found map[string][]byte
		lockKey := make([]byte, 0, tablecodec.RecordRowKeyLen)
		lockKey = append(lockKey, handleRange.startKey[:tablecodec.TableSplitKeyLen]...)
		if !w.table.Meta().HasClusteredIndex() && len(w.rowRecords) > 0 {
			failpoint.InjectCall("PartitionBackfillNonClustered", w.rowRecords[0].vals)
			// we must check if old IDs already been written,
			// i.e. double written by StateWriteOnly or StateWriteReorganization.

			// TODO: test how to use PresumeKeyNotExists/NeedConstraintCheckInPrewrite/DO_CONSTRAINT_CHECK
			// to delay the check until commit.
			// And handle commit errors and fall back to this method of checking all keys to see if we need to skip any.
			newKeys := make([]kv.Key, 0, len(w.rowRecords))
			for i := range w.rowRecords {
				newKeys = append(newKeys, w.rowRecords[i].key)
			}
			found, err = txn.BatchGet(ctx, newKeys)
			if err != nil {
				return errors.Trace(err)
			}

			// TODO: Add test that kills (like `kill -9`) the currently running
			// ddl owner, to see how it handles re-running this backfill when some batches has
			// committed and reorgInfo has not been updated, so it needs to redo some batches.
		}
		tmpRow := make([]types.Datum, len(w.reorgedTbl.Cols()))

		for _, prr := range w.rowRecords {
			taskCtx.scanCount++
			key := prr.key
			lockKey = lockKey[:tablecodec.TableSplitKeyLen]
			lockKey = append(lockKey, key[tablecodec.TableSplitKeyLen:]...)
			// Lock the *old* key, since there can still be concurrent update happening on
			// the rows from fetchRowColVals(). If we cannot lock the keys in this
			// transaction and succeed when committing, then another transaction did update
			// the same key, and we will fail and retry. When retrying, this key would be found
			// through BatchGet and skipped.
			// TODO: would it help to accumulate the keys in a slice and then only call this once?
			err = txn.LockKeys(context.Background(), new(kv.LockCtx), lockKey)
			if err != nil {
				return errors.Trace(err)
			}

			if vals, ok := found[string(key)]; ok {
				if len(vals) == len(prr.vals) && bytes.Equal(vals, prr.vals) {
					// Already backfilled or double written earlier by concurrent DML
					continue
				}
				// Not same row, due to earlier EXCHANGE PARTITION.
				// Update the current read row by Remove it and Add it back (which will give it a new _tidb_rowid)
				// which then also will be used as unique id in the new partition.
				var h kv.Handle
				var currPartID int64
				currPartID, h, err = tablecodec.DecodeRecordKey(lockKey)
				if err != nil {
					return errors.Trace(err)
				}
				_, err = w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, h, prr.vals, w.loc, w.rowMap)
				if err != nil {
					return errors.Trace(err)
				}
				for _, col := range w.table.WritableCols() {
					d, ok := w.rowMap[col.ID]
					if !ok {
						return dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
					}
					tmpRow[col.Offset] = d
				}
				// Use RemoveRecord/AddRecord to keep the indexes in-sync!
				pt := w.table.GetPartitionedTable().GetPartition(currPartID)
				err = pt.RemoveRecord(w.tblCtx, txn, h, tmpRow)
				if err != nil {
					return errors.Trace(err)
				}
				h, err = pt.AddRecord(w.tblCtx, txn, tmpRow)
				if err != nil {
					return errors.Trace(err)
				}
				w.cleanRowMap()
				// tablecodec.prefixLen is not exported, but is just TableSplitKeyLen + 2 ("_r")
				key = tablecodec.EncodeRecordKey(key[:tablecodec.TableSplitKeyLen+2], h)
				// OK to only do txn.Set() for the new partition, and defer creating the indexes,
				// since any DML changes the record it will also update or create the indexes,
				// by doing RemoveRecord+UpdateRecord
			}
			err = txn.Set(key, prr.vals)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "BackfillData", 3000)

	return
}

func (w *reorgPartitionWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) (kv.Key, bool, error) {
	w.rowRecords = w.rowRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	sysTZ := w.loc

	tmpRow := make([]types.Datum, len(w.reorgedTbl.Cols()))
	var lastAccessedHandle kv.Key
	oprStartTime := startTime
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, w.table.RecordPrefix(), txn.StartTS(), taskRange.startKey, taskRange.endKey,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in reorgPartitionWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

			if taskDone || len(w.rowRecords) >= w.batchCnt {
				return false, nil
			}

			_, err := w.rowDecoder.DecodeTheExistedColumnMap(w.exprCtx, handle, rawRow, sysTZ, w.rowMap)
			if err != nil {
				return false, errors.Trace(err)
			}

			// Set all partitioning columns and calculate which partition to write to
			for colID, offset := range w.writeColOffsetMap {
				d, ok := w.rowMap[colID]
				if !ok {
					return false, dbterror.ErrUnsupportedReorganizePartition.GenWithStackByArgs()
				}
				tmpRow[offset] = d
			}
			p, err := w.reorgedTbl.GetPartitionByRow(w.exprCtx.GetEvalCtx(), tmpRow)
			if err != nil {
				return false, errors.Trace(err)
			}
			newKey := tablecodec.EncodeTablePrefix(p.GetPhysicalID())
			newKey = append(newKey, recordKey[tablecodec.TableSplitKeyLen:]...)
			w.rowRecords = append(w.rowRecords, &rowRecord{key: newKey, vals: rawRow})

			w.cleanRowMap()
			lastAccessedHandle = recordKey
			if recordKey.Cmp(taskRange.endKey) == 0 {
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.rowRecords) == 0 {
		taskDone = true
	}

	logutil.DDLLogger().Debug("txn fetches handle info",
		zap.Uint64("txnStartTS", txn.StartTS()),
		zap.Stringer("taskRange", &taskRange),
		zap.Duration("takeTime", time.Since(startTime)))
	return getNextHandleKey(taskRange, taskDone, lastAccessedHandle), taskDone, errors.Trace(err)
}

func (w *reorgPartitionWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

func (w *reorgPartitionWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*reorgPartitionWorker) String() string {
	return typeReorgPartitionWorker.String()
}

func (w *reorgPartitionWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func (w *worker) reorgPartitionDataAndIndex(
	jobCtx *jobContext,
	t table.Table,
	reorgInfo *reorgInfo,
) (err error) {
	// First copy all table data to the new AddingDefinitions partitions
	// from each of the DroppingDefinitions partitions.
	// Then create all indexes on the AddingDefinitions partitions,
	// both new local and new global indexes
	// And last update new global indexes from the non-touched partitions
	// Note it is hard to update global indexes in-place due to:
	//   - Transactions on different TiDB nodes/domains may see different states of the table/partitions
	//   - We cannot have multiple partition ids for a unique index entry.

	// Copy the data from the DroppingDefinitions to the AddingDefinitions
	if bytes.Equal(reorgInfo.currElement.TypeKey, meta.ColumnElementKey) {
		err = w.updatePhysicalTableRow(jobCtx.stepCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		if len(reorgInfo.elements) <= 1 {
			// No indexes to (re)create, all done!
			return nil
		}
	}

	failpoint.Inject("reorgPartitionAfterDataCopy", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test in reorgPartitionAfterDataCopy")
		}
	})

	if !bytes.Equal(reorgInfo.currElement.TypeKey, meta.IndexElementKey) {
		// row data has been copied, now proceed with creating the indexes
		// on the new AddingDefinitions partitions
		reorgInfo.PhysicalTableID = t.Meta().Partition.AddingDefinitions[0].ID
		reorgInfo.currElement = reorgInfo.elements[1]
		var physTbl table.PhysicalTable
		if tbl, ok := t.(table.PartitionedTable); ok {
			physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
		} else if tbl, ok := t.(table.PhysicalTable); ok {
			// This may be used when partitioning a non-partitioned table
			physTbl = tbl
		}
		// Get the original start handle and end handle.
		currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
		if err != nil {
			return errors.Trace(err)
		}
		startHandle, endHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
		if err != nil {
			return errors.Trace(err)
		}

		// Always (re)start with the full PhysicalTable range
		reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle

		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.Int64("partitionTableId", physTbl.GetPhysicalID()),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
	}

	pi := t.Meta().GetPartitionInfo()
	if _, err = findNextPartitionID(reorgInfo.PhysicalTableID, pi.AddingDefinitions); err == nil {
		// Now build all the indexes in the new partitions.
		err = w.addTableIndex(jobCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		// All indexes are up-to-date for new partitions,
		// now we only need to add the existing non-touched partitions
		// to the global indexes
		reorgInfo.elements = reorgInfo.elements[:0]
		for _, indexInfo := range t.Meta().Indices {
			if indexInfo.Global && indexInfo.State == model.StateWriteReorganization {
				reorgInfo.elements = append(reorgInfo.elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
			}
		}
		if len(reorgInfo.elements) == 0 {
			reorgInfo.PhysicalTableID = 0
		}
		if reorgInfo.PhysicalTableID != 0 {
			reorgInfo.currElement = reorgInfo.elements[0]
			pid := pi.Definitions[0].ID
			if _, err = findNextPartitionID(pid, pi.DroppingDefinitions); err == nil {
				// Skip all dropped partitions
				pid, err = findNextNonTouchedPartitionID(pid, pi)
				if err != nil {
					return errors.Trace(err)
				}
			}
			// if pid == 0 => All partitions will be dropped, nothing more to add to global indexes.
			reorgInfo.PhysicalTableID = pid
		}
		if reorgInfo.PhysicalTableID != 0 {
			var physTbl table.PhysicalTable
			if tbl, ok := t.(table.PartitionedTable); ok {
				physTbl = tbl.GetPartition(reorgInfo.PhysicalTableID)
			} else if tbl, ok := t.(table.PhysicalTable); ok {
				// This may be used when partitioning a non-partitioned table
				physTbl = tbl
			}
			// Get the original start handle and end handle.
			currentVer, err := getValidCurrentVersion(reorgInfo.jobCtx.store)
			if err != nil {
				return errors.Trace(err)
			}
			startHandle, endHandle, err := getTableRange(reorgInfo.NewJobContext(), reorgInfo.jobCtx.store, physTbl, currentVer.Ver, reorgInfo.Job.Priority)
			if err != nil {
				return errors.Trace(err)
			}

			// Always (re)start with the full PhysicalTable range
			reorgInfo.StartKey, reorgInfo.EndKey = startHandle, endHandle
		}

		// Write the reorg info to store so the whole reorganize process can recover from panic.
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("update column and indexes",
			zap.Int64("jobID", reorgInfo.Job.ID),
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.String("startHandle", hex.EncodeToString(reorgInfo.StartKey)),
			zap.String("endHandle", hex.EncodeToString(reorgInfo.EndKey)))
		if err != nil {
			return errors.Trace(err)
		}
	}
	if _, err = findNextNonTouchedPartitionID(reorgInfo.PhysicalTableID, pi); err == nil {
		err = w.addTableIndex(jobCtx, t, reorgInfo)
		if err != nil {
			return errors.Trace(err)
		}
		reorgInfo.PhysicalTableID = 0
		err = reorgInfo.UpdateReorgMeta(reorgInfo.StartKey, w.sessPool)
		logutil.DDLLogger().Info("Non touched partitions done",
			zap.Int64("jobID", reorgInfo.Job.ID), zap.Error(err))
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func bundlesForExchangeTablePartition(t *meta.Mutator, pt *model.TableInfo, newPar *model.PartitionDefinition, nt *model.TableInfo) ([]*placement.Bundle, error) {
	bundles := make([]*placement.Bundle, 0, 3)

	ptBundle, err := placement.NewTableBundle(t, pt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ptBundle != nil {
		bundles = append(bundles, ptBundle)
	}

	parBundle, err := placement.NewPartitionBundle(t, *newPar)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if parBundle != nil {
		bundles = append(bundles, parBundle)
	}

	ntBundle, err := placement.NewTableBundle(t, nt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ntBundle != nil {
		bundles = append(bundles, ntBundle)
	}

	if parBundle == nil && ntBundle != nil {
		// newPar.ID is the ID of old table to exchange, so ntBundle != nil means it has some old placement settings.
		// We should remove it in this situation
		bundles = append(bundles, placement.NewBundle(newPar.ID))
	}

	if parBundle != nil && ntBundle == nil {
		// nt.ID is the ID of old partition to exchange, so parBundle != nil means it has some old placement settings.
		// We should remove it in this situation
		bundles = append(bundles, placement.NewBundle(nt.ID))
	}

	return bundles, nil
}

func checkExchangePartitionRecordValidation(
	ctx context.Context,
	w *worker,
	ptbl, ntbl table.Table,
	pschemaName, nschemaName, partitionName string,
) error {
	verifyFunc := func(sql string, params ...any) error {
		sctx, err := w.sessPool.Get()
		if err != nil {
			return errors.Trace(err)
		}
		defer w.sessPool.Put(sctx)

		rows, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
			ctx,
			nil,
			sql,
			params...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := len(rows)
		if rowCount != 0 {
			return errors.Trace(dbterror.ErrRowDoesNotMatchPartition)
		}
		// Check warnings!
		// Is it possible to check how many rows where checked as well?
		return nil
	}
	genConstraintCondition := func(constraints []*table.Constraint) string {
		var buf strings.Builder
		buf.WriteString("not (")
		for i, cons := range constraints {
			if i != 0 {
				buf.WriteString(" and ")
			}
			buf.WriteString(fmt.Sprintf("(%s)", cons.ExprString))
		}
		buf.WriteString(")")
		return buf.String()
	}
	type CheckConstraintTable interface {
		WritableConstraint() []*table.Constraint
	}

	pt := ptbl.Meta()
	index, _, err := getPartitionDef(pt, partitionName)
	if err != nil {
		return errors.Trace(err)
	}

	var buf strings.Builder
	buf.WriteString("select 1 from %n.%n where ")
	paramList := []any{nschemaName, ntbl.Meta().Name.L}
	checkNt := true

	pi := pt.Partition
	switch pi.Type {
	case ast.PartitionTypeHash:
		if pi.Num == 1 {
			checkNt = false
		} else {
			buf.WriteString("mod(")
			buf.WriteString(pi.Expr)
			buf.WriteString(", %?) != %?")
			paramList = append(paramList, pi.Num, index)
			if index != 0 {
				// TODO: if hash result can't be NULL, we can remove the check part.
				// For example hash(id), but id is defined not NULL.
				buf.WriteString(" or mod(")
				buf.WriteString(pi.Expr)
				buf.WriteString(", %?) is null")
				paramList = append(paramList, pi.Num, index)
			}
		}
	case ast.PartitionTypeRange:
		// Table has only one partition and has the maximum value
		if len(pi.Definitions) == 1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
			checkNt = false
		} else {
			// For range expression and range columns
			if len(pi.Columns) == 0 {
				conds, params := buildCheckSQLConditionForRangeExprPartition(pi, index)
				buf.WriteString(conds)
				paramList = append(paramList, params...)
			} else {
				conds, params := buildCheckSQLConditionForRangeColumnsPartition(pi, index)
				buf.WriteString(conds)
				paramList = append(paramList, params...)
			}
		}
	case ast.PartitionTypeList:
		if len(pi.Columns) == 0 {
			conds := buildCheckSQLConditionForListPartition(pi, index)
			buf.WriteString(conds)
		} else {
			conds := buildCheckSQLConditionForListColumnsPartition(pi, index)
			buf.WriteString(conds)
		}
	default:
		return dbterror.ErrUnsupportedPartitionType.GenWithStackByArgs(pt.Name.O)
	}

	if vardef.EnableCheckConstraint.Load() {
		pcc, ok := ptbl.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert table partition failed")
		}
		pCons := pcc.WritableConstraint()
		if len(pCons) > 0 {
			if !checkNt {
				checkNt = true
			} else {
				buf.WriteString(" or ")
			}
			buf.WriteString(genConstraintCondition(pCons))
		}
	}
	// Check non-partition table records.
	if checkNt {
		buf.WriteString(" limit 1")
		err = verifyFunc(buf.String(), paramList...)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Check partition table records.
	if vardef.EnableCheckConstraint.Load() {
		ncc, ok := ntbl.(CheckConstraintTable)
		if !ok {
			return errors.Errorf("exchange partition process assert table partition failed")
		}
		nCons := ncc.WritableConstraint()
		if len(nCons) > 0 {
			buf.Reset()
			buf.WriteString("select 1 from %n.%n partition(%n) where ")
			buf.WriteString(genConstraintCondition(nCons))
			buf.WriteString(" limit 1")
			err = verifyFunc(buf.String(), pschemaName, pt.Name.L, partitionName)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func checkExchangePartitionPlacementPolicy(t *meta.Mutator, ntPPRef, ptPPRef, partPPRef *model.PolicyRefInfo) error {
	partitionPPRef := partPPRef
	if partitionPPRef == nil {
		partitionPPRef = ptPPRef
	}

	if ntPPRef == nil && partitionPPRef == nil {
		return nil
	}
	if ntPPRef == nil || partitionPPRef == nil {
		return dbterror.ErrTablesDifferentMetadata
	}

	ptPlacementPolicyInfo, _ := getPolicyInfo(t, partitionPPRef.ID)
	ntPlacementPolicyInfo, _ := getPolicyInfo(t, ntPPRef.ID)
	if ntPlacementPolicyInfo == nil && ptPlacementPolicyInfo == nil {
		return nil
	}
	if ntPlacementPolicyInfo == nil || ptPlacementPolicyInfo == nil {
		return dbterror.ErrTablesDifferentMetadata
	}
	if ntPlacementPolicyInfo.Name.L != ptPlacementPolicyInfo.Name.L {
		return dbterror.ErrTablesDifferentMetadata
	}

	return nil
}

func buildCheckSQLConditionForRangeExprPartition(pi *model.PartitionInfo, index int) (string, []any) {
	var buf strings.Builder
	paramList := make([]any, 0, 2)
	// Since the pi.Expr string may contain the identifier, which couldn't be escaped in our ParseWithParams(...)
	// So we write it to the origin sql string here.
	if index == 0 {
		// TODO: Handle MAXVALUE in first partition
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %?")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index].LessThan[0]))
	} else if index == len(pi.Definitions)-1 && strings.EqualFold(pi.Definitions[index].LessThan[0], partitionMaxValue) {
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" is null")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index-1].LessThan[0]))
	} else {
		buf.WriteString(pi.Expr)
		buf.WriteString(" < %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" >= %? or ")
		buf.WriteString(pi.Expr)
		buf.WriteString(" is null")
		paramList = append(paramList, driver.UnwrapFromSingleQuotes(pi.Definitions[index-1].LessThan[0]), driver.UnwrapFromSingleQuotes(pi.Definitions[index].LessThan[0]))
	}
	return buf.String(), paramList
}

func buildCheckSQLConditionForRangeColumnsPartition(pi *model.PartitionInfo, index int) (string, []any) {
	var buf strings.Builder
	paramList := make([]any, 0, len(pi.Columns)*2)

	hasLowerBound := index > 0
	needOR := false

	// Lower bound check (for all partitions except first)
	if hasLowerBound {
		currVals := pi.Definitions[index-1].LessThan
		for i := range pi.Columns {
			nextIsMax := false
			if i < (len(pi.Columns)-1) && strings.EqualFold(currVals[i+1], partitionMaxValue) {
				nextIsMax = true
			}
			if needOR {
				buf.WriteString(" OR ")
			}
			if i > 0 {
				buf.WriteString("(")
				// All previous columns must be equal and non-NULL
				for j := range i {
					if j > 0 {
						buf.WriteString(" AND ")
					}
					buf.WriteString("(%n = %?)")
					paramList = append(paramList, pi.Columns[j].L, driver.UnwrapFromSingleQuotes(currVals[j]))
				}
				buf.WriteString(" AND ")
			}
			paramList = append(paramList, pi.Columns[i].L, driver.UnwrapFromSingleQuotes(currVals[i]), pi.Columns[i].L)
			if nextIsMax {
				buf.WriteString("(%n <= %? OR %n IS NULL)")
			} else {
				buf.WriteString("(%n < %? OR %n IS NULL)")
			}
			if i > 0 {
				buf.WriteString(")")
			}
			needOR = true
			if nextIsMax {
				break
			}
		}
	}

	currVals := pi.Definitions[index].LessThan
	// Upper bound check (for all partitions)
	for i := range pi.Columns {
		if strings.EqualFold(currVals[i], partitionMaxValue) {
			break
		}
		if needOR {
			buf.WriteString(" OR ")
		}
		if i > 0 {
			buf.WriteString("(")
			// All previous columns must be equal
			for j := range i {
				if j > 0 {
					buf.WriteString(" AND ")
				}
				paramList = append(paramList, pi.Columns[j].L, driver.UnwrapFromSingleQuotes(currVals[j]))
				buf.WriteString("(%n = %?)")
			}
			buf.WriteString(" AND ")
		}
		isLast := i == len(pi.Columns)-1
		if isLast {
			buf.WriteString("(%n >= %?)")
		} else {
			buf.WriteString("(%n > %?)")
		}
		paramList = append(paramList, pi.Columns[i].L, driver.UnwrapFromSingleQuotes(currVals[i]))
		if i > 0 {
			buf.WriteString(")")
		}
		needOR = true
	}

	return buf.String(), paramList
}

func buildCheckSQLConditionForListPartition(pi *model.PartitionInfo, index int) string {
	var buf strings.Builder
	// TODO: Handle DEFAULT partition
	buf.WriteString("not (")
	for i, inValue := range pi.Definitions[index].InValues {
		if i != 0 {
			buf.WriteString(" OR ")
		}
		// AND has higher priority than OR, so no need for parentheses
		for j, val := range inValue {
			if j != 0 {
				// Should never happen, since there should be no multi-columns, only a single expression :)
				buf.WriteString(" AND ")
			}
			// null-safe compare '<=>'
			buf.WriteString(fmt.Sprintf("(%s) <=> %s", pi.Expr, val))
		}
	}
	buf.WriteString(")")
	return buf.String()
}

func buildCheckSQLConditionForListColumnsPartition(pi *model.PartitionInfo, index int) string {
	var buf strings.Builder
	// TODO: Verify if this is correct!!!
	// TODO: Handle DEFAULT partition!
	// TODO: use paramList with column names, instead of quoting.
	// How to find a match?
	// (row <=> vals1) OR (row <=> vals2)
	// How to find a non-matching row:
	// NOT ( (row <=> vals1) OR (row <=> vals2) ... )
	buf.WriteString("not (")
	colNames := make([]string, 0, len(pi.Columns))
	for i := range pi.Columns {
		// TODO: Add test for this!
		// TODO: check if there are no proper quoting function for this?
		// TODO: Maybe Sprintf("%#q", str) ?
		n := "`" + strings.ReplaceAll(pi.Columns[i].O, "`", "``") + "`"
		colNames = append(colNames, n)
	}
	for i, colValues := range pi.Definitions[index].InValues {
		if i != 0 {
			buf.WriteString(" OR ")
		}
		// AND has higher priority than OR, so no need for parentheses
		for j, val := range colValues {
			if j != 0 {
				buf.WriteString(" AND ")
			}
			// null-safe compare '<=>'
			buf.WriteString(fmt.Sprintf("%s <=> %s", colNames[j], val))
		}
	}
	buf.WriteString(")")
	return buf.String()
}

func checkAddPartitionTooManyPartitions(piDefs uint64) error {
	if piDefs > uint64(mysql.PartitionCountLimit) {
		return errors.Trace(dbterror.ErrTooManyPartitions)
	}
	return nil
}

func checkAddPartitionOnTemporaryMode(tbInfo *model.TableInfo) error {
	if tbInfo.Partition != nil && tbInfo.TempTableType != model.TempTableNone {
		return dbterror.ErrPartitionNoTemporary
	}
	return nil
}

func checkPartitionColumnsUnique(tbInfo *model.TableInfo) error {
	if len(tbInfo.Partition.Columns) <= 1 {
		return nil
	}
	var columnsMap = make(map[string]struct{})
	for _, col := range tbInfo.Partition.Columns {
		if _, ok := columnsMap[col.L]; ok {
			return dbterror.ErrSameNamePartitionField.GenWithStackByArgs(col.L)
		}
		columnsMap[col.L] = struct{}{}
	}
	return nil
}

func checkNoHashPartitions(partitionNum uint64) error {
	if partitionNum == 0 {
		return ast.ErrNoParts.GenWithStackByArgs("partitions")
	}
	return nil
}

func getPartitionIDs(table *model.TableInfo) []int64 {
	if table.GetPartitionInfo() == nil {
		return []int64{}
	}
	physicalTableIDs := make([]int64, 0, len(table.Partition.Definitions))
	for _, def := range table.Partition.Definitions {
		physicalTableIDs = append(physicalTableIDs, def.ID)
	}
	return physicalTableIDs
}

func getPartitionRuleIDs(dbName string, table *model.TableInfo) []string {
	if table.GetPartitionInfo() == nil {
		return []string{}
	}
	partRuleIDs := make([]string, 0, len(table.Partition.Definitions))
	for _, def := range table.Partition.Definitions {
		partRuleIDs = append(partRuleIDs, fmt.Sprintf(label.PartitionIDFormat, label.IDPrefix, dbName, table.Name.L, def.Name.L))
	}
	return partRuleIDs
}

// checkPartitioningKeysConstraints checks that the range partitioning key is included in the table constraint.
func checkPartitioningKeysConstraints(ctx *metabuild.Context, s *ast.CreateTableStmt, tblInfo *model.TableInfo) error {
	// Returns directly if there are no unique keys in the table.
	if len(tblInfo.Indices) == 0 && !tblInfo.PKIsHandle {
		return nil
	}

	partCols, err := getPartitionColSlices(ctx.GetExprCtx(), tblInfo, s.Partition)
	if err != nil {
		return errors.Trace(err)
	}

	// Checks that the partitioning key is included in the constraint.
	// Every unique key on the table must use every column in the table's partitioning expression.
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	for _, index := range tblInfo.Indices {
		if index.Unique && !checkUniqueKeyIncludePartKey(partCols, index.Columns) {
			if index.Primary && tblInfo.IsCommonHandle {
				// global index does not support clustered index
				return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
			}
		}
	}
	// when PKIsHandle, tblInfo.Indices will not contain the primary key.
	if tblInfo.PKIsHandle {
		indexCols := []*model.IndexColumn{{
			Name:   tblInfo.GetPkName(),
			Length: types.UnspecifiedLength,
		}}
		if !checkUniqueKeyIncludePartKey(partCols, indexCols) {
			return dbterror.ErrUniqueKeyNeedAllFieldsInPf.GenWithStackByArgs("CLUSTERED INDEX")
		}
	}
	return nil
}

func checkPartitionKeysConstraint(pi *model.PartitionInfo, indexColumns []*model.IndexColumn, tblInfo *model.TableInfo) (bool, error) {
	var (
		partCols []*model.ColumnInfo
		err      error
	)
	if pi.Type == ast.PartitionTypeNone {
		return true, nil
	}
	// The expr will be an empty string if the partition is defined by:
	// CREATE TABLE t (...) PARTITION BY RANGE COLUMNS(...)
	if partExpr := pi.Expr; partExpr != "" {
		// Parse partitioning key, extract the column names in the partitioning key to slice.
		partCols, err = extractPartitionColumns(partExpr, tblInfo)
		if err != nil {
			return false, err
		}
	} else {
		partCols = make([]*model.ColumnInfo, 0, len(pi.Columns))
		for _, col := range pi.Columns {
			colInfo := tblInfo.FindPublicColumnByName(col.L)
			if colInfo == nil {
				return false, infoschema.ErrColumnNotExists.GenWithStackByArgs(col, tblInfo.Name)
			}
			partCols = append(partCols, colInfo)
		}
	}

	// In MySQL, every unique key on the table must use every column in the table's
	// partitioning expression.(This also includes the table's primary key.)
	// See https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations-partitioning-keys-unique-keys.html
	// TiDB can remove this limitation with Global Index
	return checkUniqueKeyIncludePartKey(columnInfoSlice(partCols), indexColumns), nil
}

type columnNameExtractor struct {
	extractedColumns []*model.ColumnInfo
	tblInfo          *model.TableInfo
	err              error
}

func (*columnNameExtractor) Enter(node ast.Node) (ast.Node, bool) {
	return node, false
}

func (cne *columnNameExtractor) Leave(node ast.Node) (ast.Node, bool) {
	if c, ok := node.(*ast.ColumnNameExpr); ok {
		info := findColumnByName(c.Name.Name.L, cne.tblInfo)
		if info != nil {
			cne.extractedColumns = append(cne.extractedColumns, info)
			return node, true
		}
		cne.err = dbterror.ErrBadField.GenWithStackByArgs(c.Name.Name.O, "expression")
		return nil, false
	}
	return node, true
}

func findColumnByName(colName string, tblInfo *model.TableInfo) *model.ColumnInfo {
	if tblInfo == nil {
		return nil
	}
	for _, info := range tblInfo.Columns {
		if info.Name.L == colName {
			return info
		}
	}
	return nil
}

func extractPartitionColumns(partExpr string, tblInfo *model.TableInfo) ([]*model.ColumnInfo, error) {
	partExpr = "select " + partExpr
	stmts, _, err := parser.New().ParseSQL(partExpr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	extractor := &columnNameExtractor{
		tblInfo:          tblInfo,
		extractedColumns: make([]*model.ColumnInfo, 0),
	}
	stmts[0].Accept(extractor)
	if extractor.err != nil {
		return nil, errors.Trace(extractor.err)
	}
	return extractor.extractedColumns, nil
}

// stringSlice is defined for checkUniqueKeyIncludePartKey.
// if Go supports covariance, the code shouldn't be so complex.
type stringSlice interface {
	Len() int
	At(i int) string
}

// checkUniqueKeyIncludePartKey checks that the partitioning key is included in the constraint.
func checkUniqueKeyIncludePartKey(partCols stringSlice, idxCols []*model.IndexColumn) bool {
	for i := range partCols.Len() {
		partCol := partCols.At(i)
		_, idxCol := model.FindIndexColumnByName(idxCols, partCol)
		if idxCol == nil {
			// Partition column is not found in the index columns.
			return false
		}
		if idxCol.Length > 0 {
			// The partition column is found in the index columns, but the index column is a prefix index
			return false
		}
	}
	return true
}

// columnInfoSlice implements the stringSlice interface.
type columnInfoSlice []*model.ColumnInfo

func (cis columnInfoSlice) Len() int {
	return len(cis)
}

func (cis columnInfoSlice) At(i int) string {
	return cis[i].Name.L
}

// columnNameSlice implements the stringSlice interface.
type columnNameSlice []*ast.ColumnName

func (cns columnNameSlice) Len() int {
	return len(cns)
}

func (cns columnNameSlice) At(i int) string {
	return cns[i].Name.L
}

func isPartExprUnsigned(ectx expression.EvalContext, tbInfo *model.TableInfo) bool {
	ctx := tables.NewPartitionExprBuildCtx()
	expr, err := expression.ParseSimpleExpr(ctx, tbInfo.Partition.Expr, expression.WithTableInfo("", tbInfo))
	if err != nil {
		logutil.DDLLogger().Error("isPartExpr failed parsing expression!", zap.Error(err))
		return false
	}
	if mysql.HasUnsignedFlag(expr.GetType(ectx).GetFlag()) {
		return true
	}
	return false
}

// truncateTableByReassignPartitionIDs reassigns new partition ids.
// it also returns the new partition IDs for cases described below.
func truncateTableByReassignPartitionIDs(t *meta.Mutator, tblInfo *model.TableInfo, pids []int64) ([]int64, error) {
	if len(pids) < len(tblInfo.Partition.Definitions) {
		// To make it compatible with older versions when pids was not given
		// and if there has been any add/reorganize partition increasing the number of partitions
		morePids, err := t.GenGlobalIDs(len(tblInfo.Partition.Definitions) - len(pids))
		if err != nil {
			return nil, errors.Trace(err)
		}
		pids = append(pids, morePids...)
	}
	newDefs := make([]model.PartitionDefinition, 0, len(tblInfo.Partition.Definitions))
	for i, def := range tblInfo.Partition.Definitions {
		newDef := def
		newDef.ID = pids[i]
		newDefs = append(newDefs, newDef)
	}
	tblInfo.Partition.Definitions = newDefs
	return pids, nil
}

type partitionExprProcessor func(expression.BuildContext, *model.TableInfo, ast.ExprNode) error

type partitionExprChecker struct {
	processors []partitionExprProcessor
	ctx        expression.BuildContext
	tbInfo     *model.TableInfo
	err        error

	columns []*model.ColumnInfo
}

func newPartitionExprChecker(ctx expression.BuildContext, tbInfo *model.TableInfo, processor ...partitionExprProcessor) *partitionExprChecker {
	p := &partitionExprChecker{processors: processor, ctx: ctx, tbInfo: tbInfo}
	p.processors = append(p.processors, p.extractColumns)
	return p
}

func (p *partitionExprChecker) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	expr, ok := n.(ast.ExprNode)
	if !ok {
		return n, true
	}
	for _, processor := range p.processors {
		if err := processor(p.ctx, p.tbInfo, expr); err != nil {
			p.err = err
			return n, true
		}
	}

	return n, false
}

func (p *partitionExprChecker) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, p.err == nil
}

func (p *partitionExprChecker) extractColumns(_ expression.BuildContext, _ *model.TableInfo, expr ast.ExprNode) error {
	columnNameExpr, ok := expr.(*ast.ColumnNameExpr)
	if !ok {
		return nil
	}
	colInfo := findColumnByName(columnNameExpr.Name.Name.L, p.tbInfo)
	if colInfo == nil {
		return errors.Trace(dbterror.ErrBadField.GenWithStackByArgs(columnNameExpr.Name.Name.L, "partition function"))
	}

	p.columns = append(p.columns, colInfo)
	return nil
}

func checkPartitionExprAllowed(_ expression.BuildContext, tb *model.TableInfo, e ast.ExprNode) error {
	switch v := e.(type) {
	case *ast.FuncCallExpr:
		if _, ok := expression.AllowedPartitionFuncMap[v.FnName.L]; ok {
			return nil
		}
	case *ast.BinaryOperationExpr:
		if _, ok := expression.AllowedPartition4BinaryOpMap[v.Op]; ok {
			return errors.Trace(checkNoTimestampArgs(tb, v.L, v.R))
		}
	case *ast.UnaryOperationExpr:
		if _, ok := expression.AllowedPartition4UnaryOpMap[v.Op]; ok {
			return errors.Trace(checkNoTimestampArgs(tb, v.V))
		}
	case *ast.ColumnNameExpr, *ast.ParenthesesExpr, *driver.ValueExpr, *ast.MaxValueExpr,
		*ast.DefaultExpr, *ast.TimeUnitExpr:
		return nil
	}
	return errors.Trace(dbterror.ErrPartitionFunctionIsNotAllowed)
}

func checkPartitionExprArgs(_ expression.BuildContext, tblInfo *model.TableInfo, e ast.ExprNode) error {
	expr, ok := e.(*ast.FuncCallExpr)
	if !ok {
		return nil
	}
	argsType, err := collectArgsType(tblInfo, expr.Args...)
	if err != nil {
		return errors.Trace(err)
	}
	switch expr.FnName.L {
	case ast.ToDays, ast.ToSeconds, ast.DayOfMonth, ast.Month, ast.DayOfYear, ast.Quarter, ast.YearWeek,
		ast.Year, ast.Weekday, ast.DayOfWeek, ast.Day:
		return errors.Trace(checkResultOK(hasDateArgs(argsType...)))
	case ast.Hour, ast.Minute, ast.Second, ast.TimeToSec, ast.MicroSecond:
		return errors.Trace(checkResultOK(hasTimeArgs(argsType...)))
	case ast.UnixTimestamp:
		return errors.Trace(checkResultOK(hasTimestampArgs(argsType...)))
	case ast.FromDays:
		return errors.Trace(checkResultOK(hasDateArgs(argsType...) || hasTimeArgs(argsType...)))
	case ast.Extract:
		switch expr.Args[0].(*ast.TimeUnitExpr).Unit {
		case ast.TimeUnitYear, ast.TimeUnitYearMonth, ast.TimeUnitQuarter, ast.TimeUnitMonth, ast.TimeUnitDay:
			return errors.Trace(checkResultOK(hasDateArgs(argsType...)))
		case ast.TimeUnitDayMicrosecond, ast.TimeUnitDayHour, ast.TimeUnitDayMinute, ast.TimeUnitDaySecond:
			return errors.Trace(checkResultOK(hasDatetimeArgs(argsType...)))
		case ast.TimeUnitHour, ast.TimeUnitHourMinute, ast.TimeUnitHourSecond, ast.TimeUnitMinute, ast.TimeUnitMinuteSecond,
			ast.TimeUnitSecond, ast.TimeUnitMicrosecond, ast.TimeUnitHourMicrosecond, ast.TimeUnitMinuteMicrosecond, ast.TimeUnitSecondMicrosecond:
			return errors.Trace(checkResultOK(hasTimeArgs(argsType...)))
		default:
			return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
		}
	case ast.DateDiff:
		return errors.Trace(checkResultOK(slice.AllOf(argsType, func(i int) bool {
			return hasDateArgs(argsType[i])
		})))

	case ast.Abs, ast.Ceiling, ast.Floor, ast.Mod:
		has := hasTimestampArgs(argsType...)
		if has {
			return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
		}
	}
	return nil
}

func collectArgsType(tblInfo *model.TableInfo, exprs ...ast.ExprNode) ([]byte, error) {
	ts := make([]byte, 0, len(exprs))
	for _, arg := range exprs {
		col, ok := arg.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		columnInfo := findColumnByName(col.Name.Name.L, tblInfo)
		if columnInfo == nil {
			return nil, errors.Trace(dbterror.ErrBadField.GenWithStackByArgs(col.Name.Name.L, "partition function"))
		}
		ts = append(ts, columnInfo.GetType())
	}

	return ts, nil
}

func hasDateArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDate || argsType[i] == mysql.TypeDatetime
	})
}

func hasTimeArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDuration || argsType[i] == mysql.TypeDatetime
	})
}

func hasTimestampArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeTimestamp
	})
}

func hasDatetimeArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDatetime
	})
}

func checkNoTimestampArgs(tbInfo *model.TableInfo, exprs ...ast.ExprNode) error {
	argsType, err := collectArgsType(tbInfo, exprs...)
	if err != nil {
		return err
	}
	if hasTimestampArgs(argsType...) {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}
	return nil
}

// hexIfNonPrint checks if printable UTF-8 characters from a single quoted string,
// if so, just returns the string
// else returns a hex string of the binary string (i.e. actual encoding, not unicode code points!)
func hexIfNonPrint(s string) string {
	isPrint := true
	// https://go.dev/blog/strings `for range` of string converts to runes!
	for _, runeVal := range s {
		if !strconv.IsPrint(runeVal) {
			isPrint = false
			break
		}
	}
	if isPrint {
		return s
	}
	// To avoid 'simple' MySQL accepted escape characters, to be showed as hex, just escape them
	// \0 \b \n \r \t \Z, see https://dev.mysql.com/doc/refman/8.0/en/string-literals.html
	isPrint = true
	res := ""
	for _, runeVal := range s {
		switch runeVal {
		case 0: // Null
			res += `\0`
		case 7: // Bell
			res += `\b`
		case '\t': // 9
			res += `\t`
		case '\n': // 10
			res += `\n`
		case '\r': // 13
			res += `\r`
		case 26: // ctrl-z / Substitute
			res += `\Z`
		default:
			if !strconv.IsPrint(runeVal) {
				isPrint = false
				break
			}
			res += string(runeVal)
		}
	}
	if isPrint {
		return res
	}
	// Not possible to create an easy interpreted MySQL string, return as hex string
	// Can be converted to string in MySQL like: CAST(UNHEX('<hex string>') AS CHAR(255))
	return "0x" + hex.EncodeToString([]byte(driver.UnwrapFromSingleQuotes(s)))
}

func writeColumnListToBuffer(partitionInfo *model.PartitionInfo, sqlMode mysql.SQLMode, buf *bytes.Buffer) {
	if partitionInfo.IsEmptyColumns {
		return
	}
	for i, col := range partitionInfo.Columns {
		buf.WriteString(stringutil.Escape(col.O, sqlMode))
		if i < len(partitionInfo.Columns)-1 {
			buf.WriteString(",")
		}
	}
}

// AppendPartitionInfo is used in SHOW CREATE TABLE as well as generation the SQL syntax
// for the PartitionInfo during validation of various DDL commands
func AppendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer, sqlMode mysql.SQLMode) {
	if partitionInfo == nil {
		return
	}
	// Since MySQL 5.1/5.5 is very old and TiDB aims for 5.7/8.0 compatibility, we will not
	// include the /*!50100 or /*!50500 comments for TiDB.
	// This also solves the issue with comments within comments that would happen for
	// PLACEMENT POLICY options.
	defaultPartitionDefinitions := true
	if partitionInfo.Type == ast.PartitionTypeHash ||
		partitionInfo.Type == ast.PartitionTypeKey {
		for i, def := range partitionInfo.Definitions {
			if def.Name.O != fmt.Sprintf("p%d", i) {
				defaultPartitionDefinitions = false
				break
			}
			if len(def.Comment) > 0 || def.PlacementPolicyRef != nil {
				defaultPartitionDefinitions = false
				break
			}
		}

		if defaultPartitionDefinitions {
			if partitionInfo.Type == ast.PartitionTypeHash {
				fmt.Fprintf(buf, "\nPARTITION BY HASH (%s) PARTITIONS %d", partitionInfo.Expr, partitionInfo.Num)
			} else {
				buf.WriteString("\nPARTITION BY KEY (")
				writeColumnListToBuffer(partitionInfo, sqlMode, buf)
				buf.WriteString(")")
				fmt.Fprintf(buf, " PARTITIONS %d", partitionInfo.Num)
			}
			return
		}
	}
	// this if statement takes care of lists/range/key columns case
	if len(partitionInfo.Columns) > 0 {
		// partitionInfo.Type == model.PartitionTypeRange || partitionInfo.Type == model.PartitionTypeList
		// || partitionInfo.Type == model.PartitionTypeKey
		// Notice that MySQL uses two spaces between LIST and COLUMNS...
		if partitionInfo.Type == ast.PartitionTypeKey {
			fmt.Fprintf(buf, "\nPARTITION BY %s (", partitionInfo.Type.String())
		} else {
			fmt.Fprintf(buf, "\nPARTITION BY %s COLUMNS(", partitionInfo.Type.String())
		}
		writeColumnListToBuffer(partitionInfo, sqlMode, buf)
		buf.WriteString(")\n(")
	} else {
		fmt.Fprintf(buf, "\nPARTITION BY %s (%s)\n(", partitionInfo.Type.String(), partitionInfo.Expr)
	}

	AppendPartitionDefs(partitionInfo, buf, sqlMode)
	buf.WriteString(")")
}

// AppendPartitionDefs generates a list of partition definitions needed for SHOW CREATE TABLE (in executor/show.go)
// as well as needed for generating the ADD PARTITION query for INTERVAL partitioning of ALTER TABLE t LAST PARTITION
// and generating the CREATE TABLE query from CREATE TABLE ... INTERVAL
func AppendPartitionDefs(partitionInfo *model.PartitionInfo, buf *bytes.Buffer, sqlMode mysql.SQLMode) {
	for i, def := range partitionInfo.Definitions {
		if i > 0 {
			fmt.Fprintf(buf, ",\n ")
		}
		fmt.Fprintf(buf, "PARTITION %s", stringutil.Escape(def.Name.O, sqlMode))
		// PartitionTypeHash and PartitionTypeKey do not have any VALUES definition
		if partitionInfo.Type == ast.PartitionTypeRange {
			lessThans := make([]string, len(def.LessThan))
			for idx, v := range def.LessThan {
				lessThans[idx] = hexIfNonPrint(v)
			}
			fmt.Fprintf(buf, " VALUES LESS THAN (%s)", strings.Join(lessThans, ","))
		} else if partitionInfo.Type == ast.PartitionTypeList {
			if len(def.InValues) == 0 {
				fmt.Fprintf(buf, " DEFAULT")
			} else if len(def.InValues) == 1 &&
				len(def.InValues[0]) == 1 &&
				strings.EqualFold(def.InValues[0][0], "DEFAULT") {
				fmt.Fprintf(buf, " DEFAULT")
			} else {
				values := bytes.NewBuffer(nil)
				for j, inValues := range def.InValues {
					if j > 0 {
						values.WriteString(",")
					}
					if len(inValues) > 1 {
						values.WriteString("(")
						tmpVals := make([]string, len(inValues))
						for idx, v := range inValues {
							tmpVals[idx] = hexIfNonPrint(v)
						}
						values.WriteString(strings.Join(tmpVals, ","))
						values.WriteString(")")
					} else if len(inValues) == 1 {
						values.WriteString(hexIfNonPrint(inValues[0]))
					}
				}
				fmt.Fprintf(buf, " VALUES IN (%s)", values.String())
			}
		}
		if len(def.Comment) > 0 {
			fmt.Fprintf(buf, " COMMENT '%s'", format.OutputFormat(def.Comment))
		}
		if def.PlacementPolicyRef != nil {
			// add placement ref info here
			fmt.Fprintf(buf, " /*T![placement] PLACEMENT POLICY=%s */", stringutil.Escape(def.PlacementPolicyRef.Name.O, sqlMode))
		}
	}
}

func generatePartValuesWithTp(partVal types.Datum, tp types.FieldType) (string, error) {
	if partVal.Kind() == types.KindNull {
		return "NULL", nil
	}

	s, err := partVal.ToString()
	if err != nil {
		return "", err
	}

	switch tp.EvalType() {
	case types.ETInt:
		return s, nil
	case types.ETString:
		// The `partVal` can be an invalid utf8 string if it's converted to BINARY, then the content will be lost after
		// marshaling and storing in the schema. In this case, we use a hex literal to work around this issue.
		if tp.GetCharset() == charset.CharsetBin && len(s) != 0 {
			return fmt.Sprintf("_binary 0x%x", s), nil
		}
		return driver.WrapInSingleQuotes(s), nil
	case types.ETDatetime, types.ETDuration:
		return driver.WrapInSingleQuotes(s), nil
	}

	return "", dbterror.ErrWrongTypeColumnValue.GenWithStackByArgs()
}

func checkPartitionDefinitionConstraints(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	var err error
	if err = checkPartitionNameUnique(tbInfo.Partition); err != nil {
		return errors.Trace(err)
	}
	if err = checkAddPartitionTooManyPartitions(uint64(len(tbInfo.Partition.Definitions))); err != nil {
		return err
	}
	if err = checkAddPartitionOnTemporaryMode(tbInfo); err != nil {
		return err
	}
	if err = checkPartitionColumnsUnique(tbInfo); err != nil {
		return err
	}

	switch tbInfo.Partition.Type {
	case ast.PartitionTypeRange:
		failpoint.Inject("CheckPartitionByRangeErr", func() {
			panic("mockCheckPartitionByRangeErr")
		})
		err = checkPartitionByRange(ctx, tbInfo)
	case ast.PartitionTypeHash, ast.PartitionTypeKey:
		err = checkPartitionByHash(tbInfo)
	case ast.PartitionTypeList:
		err = checkPartitionByList(ctx, tbInfo)
	}
	return errors.Trace(err)
}

func checkPartitionByHash(tbInfo *model.TableInfo) error {
	return checkNoHashPartitions(tbInfo.Partition.Num)
}

// checkPartitionByRange checks validity of a "BY RANGE" partition.
func checkPartitionByRange(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	pi := tbInfo.Partition

	if len(pi.Columns) == 0 {
		return checkRangePartitionValue(ctx, tbInfo)
	}

	return checkRangeColumnsPartitionValue(ctx, tbInfo)
}

func checkRangeColumnsPartitionValue(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	// Range columns partition key supports multiple data types with integer、datetime、string.
	pi := tbInfo.Partition
	defs := pi.Definitions
	if len(defs) < 1 {
		return ast.ErrPartitionsMustBeDefined.GenWithStackByArgs("RANGE")
	}

	curr := &defs[0]
	if len(curr.LessThan) != len(pi.Columns) {
		return errors.Trace(ast.ErrPartitionColumnList)
	}
	var prev *model.PartitionDefinition
	for i := 1; i < len(defs); i++ {
		prev, curr = curr, &defs[i]
		succ, err := checkTwoRangeColumns(ctx, curr, prev, pi, tbInfo)
		if err != nil {
			return err
		}
		if !succ {
			return errors.Trace(dbterror.ErrRangeNotIncreasing)
		}
	}
	return nil
}

func checkTwoRangeColumns(ctx expression.BuildContext, curr, prev *model.PartitionDefinition, pi *model.PartitionInfo, tbInfo *model.TableInfo) (bool, error) {
	if len(curr.LessThan) != len(pi.Columns) {
		return false, errors.Trace(ast.ErrPartitionColumnList)
	}
	for i := range pi.Columns {
		// Special handling for MAXVALUE.
		if strings.EqualFold(curr.LessThan[i], partitionMaxValue) && !strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// If current is maxvalue, it certainly >= previous.
			return true, nil
		}
		if strings.EqualFold(prev.LessThan[i], partitionMaxValue) {
			// Current is not maxvalue, and previous is maxvalue.
			return false, nil
		}

		// The tuples of column values used to define the partitions are strictly increasing:
		// PARTITION p0 VALUES LESS THAN (5,10,'ggg')
		// PARTITION p1 VALUES LESS THAN (10,20,'mmm')
		// PARTITION p2 VALUES LESS THAN (15,30,'sss')
		colInfo := findColumnByName(pi.Columns[i].L, tbInfo)
		cmp, err := parseAndEvalBoolExpr(ctx, curr.LessThan[i], prev.LessThan[i], colInfo, tbInfo)
		if err != nil {
			return false, err
		}

		if cmp > 0 {
			return true, nil
		}

		if cmp < 0 {
			return false, nil
		}
	}
	return false, nil
}

// equal, return 0
// greater, return 1
// less, return -1
func parseAndEvalBoolExpr(ctx expression.BuildContext, l, r string, colInfo *model.ColumnInfo, tbInfo *model.TableInfo) (int64, error) {
	lexpr, err := expression.ParseSimpleExpr(ctx, l, expression.WithTableInfo("", tbInfo), expression.WithCastExprTo(&colInfo.FieldType))
	if err != nil {
		return 0, err
	}
	rexpr, err := expression.ParseSimpleExpr(ctx, r, expression.WithTableInfo("", tbInfo), expression.WithCastExprTo(&colInfo.FieldType))
	if err != nil {
		return 0, err
	}

	e, err := expression.NewFunctionBase(ctx, ast.EQ, field_types.NewFieldType(mysql.TypeLonglong), lexpr, rexpr)
	if err != nil {
		return 0, err
	}
	e.SetCharsetAndCollation(colInfo.GetCharset(), colInfo.GetCollate())
	res, _, err1 := e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err1 != nil {
		return 0, err1
	}
	if res == 1 {
		return 0, nil
	}

	e, err = expression.NewFunctionBase(ctx, ast.GT, field_types.NewFieldType(mysql.TypeLonglong), lexpr, rexpr)
	if err != nil {
		return 0, err
	}
	e.SetCharsetAndCollation(colInfo.GetCharset(), colInfo.GetCollate())
	res, _, err1 = e.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err1 != nil {
		return 0, err1
	}
	if res > 0 {
		return 1, nil
	}
	return -1, nil
}

// checkPartitionByList checks validity of a "BY LIST" partition.
func checkPartitionByList(ctx expression.BuildContext, tbInfo *model.TableInfo) error {
	return checkListPartitionValue(ctx, tbInfo)
}
