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

package logicalop

import (
	"slices"
	"strconv"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

var _ base.LogicalPlan = &BaseLogicalPlan{}

const (
	// ApplyGenFromXFDeCorrelateRuleFlag is the flag marked for this op apply is intermediary.
	ApplyGenFromXFDeCorrelateRuleFlag uint64 = 1 << 0
)

// BaseLogicalPlan is the common structure that used in logical plan.
type BaseLogicalPlan struct {
	baseimpl.Plan

	taskMap map[string]base.Task
	// taskMapBak forms a backlog stack of taskMap, used to roll back the taskMap.
	taskMapBak []string
	// planIDsHash is the hash of the subtree root from this logical plan.
	planIDsHash uint64
	// taskMapBakTS stores the timestamps of logs.
	taskMapBakTS []uint64
	self         base.LogicalPlan
	maxOneRow    bool
	children     []base.LogicalPlan
	// fdSet is a set of functional dependencies(FDs) which powers many optimizations,
	// including eliminating unnecessary DISTINCT operators, simplifying ORDER BY columns,
	// removing Max1Row operators, and mapping semi-joins to inner-joins.
	// for now, it's hard to maintain in individual operator, build it from bottom up when using.
	fdSet *fd.FDSet

	// Flag is with that each bit has its meaning to mark this logical plan for special handling.
	Flag uint64
}

// *************************** implementation of HashEquals interface ***************************

// Hash64 implements HashEquals.<0th> interface.
func (p *BaseLogicalPlan) Hash64(h base2.Hasher) {
	_, ok1 := p.self.(*LogicalSequence)
	_, ok2 := p.self.(*LogicalMaxOneRow)
	if !ok1 && !ok2 {
		intest.Assert(false, "Hash64 should not be called directly")
	}
	h.HashInt(p.ID())
}

// Equals implements HashEquals.<1st> interface.
func (p *BaseLogicalPlan) Equals(other any) bool {
	_, ok1 := p.self.(*LogicalSequence)
	_, ok2 := p.self.(*LogicalMaxOneRow)
	if !ok1 && !ok2 {
		intest.Assert(false, "Equals should not be called directly")
	}
	if other == nil {
		return false
	}
	olp, ok := other.(*BaseLogicalPlan)
	if !ok {
		return false
	}
	return p.ID() == olp.ID()
}

// *************************** implementation of base Plan interface ***************************

// ExplainInfo implements Plan interface.
func (*BaseLogicalPlan) ExplainInfo() string {
	return ""
}

// Schema implements Plan Schema interface.
func (p *BaseLogicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// OutputNames implements Plan Schema interface.
func (p *BaseLogicalPlan) OutputNames() types.NameSlice {
	return p.children[0].OutputNames()
}

// SetOutputNames implements Plan Schema interface.
func (p *BaseLogicalPlan) SetOutputNames(names types.NameSlice) {
	p.children[0].SetOutputNames(names)
}

// BuildPlanTrace implements Plan
func (p *BaseLogicalPlan) BuildPlanTrace() *tracing.PlanTrace {
	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: p.TP(), ExplainInfo: p.self.ExplainInfo()}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.BuildPlanTrace())
	}
	return planTrace
}

// *************************** implementation of logicalPlan interface ***************************

// HashCode implements LogicalPlan.<0th> interface.
func (p *BaseLogicalPlan) HashCode() []byte {
	// We use PlanID for the default hash, so if two plans do not have
	// the same id, the hash value will never be the same.
	result := make([]byte, 0, 4)
	result = util.EncodeIntAsUint32(result, p.ID())
	return result
}

// PredicatePushDown implements LogicalPlan.<1st> interface.
func (p *BaseLogicalPlan) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) ([]expression.Expression, base.LogicalPlan, error) {
	if len(p.children) == 0 {
		return predicates, p.self, nil
	}
	child := p.children[0]
	rest, newChild, err := child.PredicatePushDown(predicates, opt)
	if err != nil {
		return nil, p.self, err
	}
	AddSelection(p.self, newChild, rest, 0, opt)
	return nil, p.self, nil
}

// PruneColumns implements LogicalPlan.<2nd> interface.
func (p *BaseLogicalPlan) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	if len(p.children) == 0 {
		return p.self, nil
	}
	var err error
	p.children[0], err = p.children[0].PruneColumns(parentUsedCols, opt)
	if err != nil {
		return nil, err
	}
	return p.self, nil
}

// FindBestTask implements LogicalPlan.<3rd> interface.
func (p *BaseLogicalPlan) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp,
	opt *optimizetrace.PhysicalOptimizeOp) (bestTask base.Task, cntPlan int64, err error) {
	return utilfuncp.FindBestTask4BaseLogicalPlan(p, prop, planCounter, opt)
}

// BuildKeyInfo implements LogicalPlan.<4th> interface.
func (p *BaseLogicalPlan) BuildKeyInfo(_ *expression.Schema, _ []*expression.Schema) {
	childMaxOneRow := make([]bool, len(p.children))
	for i := range p.children {
		childMaxOneRow[i] = p.children[i].MaxOneRow()
	}
	p.maxOneRow = HasMaxOneRow(p.self, childMaxOneRow)
}

// PushDownTopN implements the LogicalPlan.<5th> interface.
func (p *BaseLogicalPlan) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	return pushDownTopNForBaseLogicalPlan(p, topNLogicalPlan, opt)
}

// DeriveTopN implements the LogicalPlan.<6th> interface.
func (p *BaseLogicalPlan) DeriveTopN(opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	s := p.self
	if s.SCtx().GetSessionVars().AllowDeriveTopN {
		for i, child := range s.Children() {
			newChild := child.DeriveTopN(opt)
			s.SetChild(i, newChild)
		}
	}
	return s
}

// PredicateSimplification implements the LogicalPlan.<7th> interface.
func (p *BaseLogicalPlan) PredicateSimplification(opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	s := p.self
	for i, child := range s.Children() {
		newChild := child.PredicateSimplification(opt)
		s.SetChild(i, newChild)
	}
	return s
}

// ConstantPropagation implements the LogicalPlan.<8th> interface.
func (*BaseLogicalPlan) ConstantPropagation(_ base.LogicalPlan, _ int, _ *optimizetrace.LogicalOptimizeOp) (newRoot base.LogicalPlan) {
	// Only LogicalJoin can apply constant propagation
	// Other Logical plan do nothing
	return nil
}

// PullUpConstantPredicates implements the LogicalPlan.<9th> interface.
func (*BaseLogicalPlan) PullUpConstantPredicates() []expression.Expression {
	// Only LogicalProjection and LogicalSelection can get constant predicates
	// Other Logical plan return nil
	return nil
}

// RecursiveDeriveStats implements LogicalPlan.<10th> interface.
func (p *BaseLogicalPlan) RecursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, bool, error) {
	childStats := make([]*property.StatsInfo, len(p.children))
	childSchema := make([]*expression.Schema, len(p.children))
	cumColGroups := p.self.ExtractColGroups(colGroups)
	reloads := make([]bool, 0, len(p.children))
	for i, child := range p.children {
		childProfile, reload, err := child.RecursiveDeriveStats(cumColGroups)
		if err != nil {
			return nil, false, err
		}
		reloads = append(reloads, reload)
		childStats[i] = childProfile
		childSchema[i] = child.Schema()
	}
	// when the child has reloaded their stats, current logical operator should reload itself too.
	return p.self.DeriveStats(childStats, p.self.Schema(), childSchema, reloads)
}

// DeriveStats implements LogicalPlan.<11th> interface.
func (p *BaseLogicalPlan) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, _ []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	for _, one := range reloads {
		reload = reload || one
	}
	if len(childStats) == 1 {
		p.SetStats(childStats[0])
		return p.StatsInfo(), true, nil
	}
	if len(childStats) > 1 {
		err := plannererrors.ErrInternal.GenWithStack("LogicalPlans with more than one child should implement their own DeriveStats().")
		return nil, false, err
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}
	profile := &property.StatsInfo{
		RowCount: float64(1),
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	}
	for _, col := range selfSchema.Columns {
		profile.ColNDVs[col.UniqueID] = 1
	}
	p.SetStats(profile)
	return profile, true, nil
}

// ExtractColGroups implements LogicalPlan.<12th> interface.
func (*BaseLogicalPlan) ExtractColGroups(_ [][]*expression.Column) [][]*expression.Column {
	return nil
}

// PreparePossibleProperties implements LogicalPlan.<13th> interface.
func (*BaseLogicalPlan) PreparePossibleProperties(_ *expression.Schema, _ ...[][]*expression.Column) [][]*expression.Column {
	return nil
}

// ExhaustPhysicalPlans implements LogicalPlan.<14th> interface.
func (*BaseLogicalPlan) ExhaustPhysicalPlans(*property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	panic("baseLogicalPlan.ExhaustPhysicalPlans() should never be called.")
}

// ExtractCorrelatedCols implements LogicalPlan.<15th> interface.
func (*BaseLogicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// MaxOneRow implements the LogicalPlan.<16th> interface.
func (p *BaseLogicalPlan) MaxOneRow() bool {
	return p.maxOneRow
}

// Children implements LogicalPlan.<17th> interface.
func (p *BaseLogicalPlan) Children() []base.LogicalPlan {
	return p.children
}

// SetChildren implements LogicalPlan.<18th> interface.
func (p *BaseLogicalPlan) SetChildren(children ...base.LogicalPlan) {
	p.children = children
}

// SetChild implements LogicalPlan.<19th> interface.
func (p *BaseLogicalPlan) SetChild(i int, child base.LogicalPlan) {
	p.children[i] = child
}

// RollBackTaskMap implements LogicalPlan.<20th> interface.
func (p *BaseLogicalPlan) RollBackTaskMap(ts uint64) {
	if !p.SCtx().GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		return
	}
	if len(p.taskMapBak) > 0 {
		// Rollback all the logs with TimeStamp TS.
		n := len(p.taskMapBak)
		for i := 0; i < n; i++ {
			cur := p.taskMapBak[i]
			if p.taskMapBakTS[i] < ts {
				continue
			}

			// Remove the i_th log.
			p.taskMapBak = slices.Delete(p.taskMapBak, i, i+1)
			p.taskMapBakTS = slices.Delete(p.taskMapBakTS, i, i+1)
			i--
			n--

			// Roll back taskMap.
			p.taskMap[cur] = nil
		}
	}
	for _, child := range p.children {
		child.RollBackTaskMap(ts)
	}
}

// CanPushToCop implements LogicalPlan.<21st> interface.
// it checks if it can be pushed to some stores. For TiKV, it only checks datasource.
// For TiFlash, it will check whether the operator is supported, but note that the check
// might be inaccurate.
// Deprecated: don't depend subtree based push check, see CanSelfBeingPushedToCopImpl based op-self push check.
func (p *BaseLogicalPlan) CanPushToCop(storeTp kv.StoreType) bool {
	return CanPushToCopImpl(p, storeTp)
}

// ExtractFD implements LogicalPlan.<22nd> interface.
// It returns the children[0]'s fdSet if there are no adding/removing fd in this logic plan.
func (p *BaseLogicalPlan) ExtractFD() *fd.FDSet {
	if p.fdSet != nil {
		return p.fdSet
	}
	fds := &fd.FDSet{HashCodeToUniqueID: make(map[string]int)}
	for _, ch := range p.children {
		fds.AddFrom(ch.ExtractFD())
	}
	return fds
}

// GetBaseLogicalPlan implements LogicalPlan.<23rd> interface.
// It returns the baseLogicalPlan inside each logical plan.
func (p *BaseLogicalPlan) GetBaseLogicalPlan() base.LogicalPlan {
	return p
}

// ConvertOuterToInnerJoin implements LogicalPlan.<24th> interface.
func (p *BaseLogicalPlan) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	s := p.self
	for i, child := range s.Children() {
		newChild := child.ConvertOuterToInnerJoin(predicates)
		s.SetChild(i, newChild)
	}
	return s
}

// *************************** implementation of self functionality ***************************

// GetLogicalTS4TaskMap get the logical TimeStamp now to help rollback the TaskMap changes after that.
func (p *BaseLogicalPlan) GetLogicalTS4TaskMap() uint64 {
	p.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS++
	return p.SCtx().GetSessionVars().StmtCtx.TaskMapBakTS
}

// GetTask returns the history recorded Task for specified property.
func (p *BaseLogicalPlan) GetTask(prop *property.PhysicalProperty) base.Task {
	key := strconv.FormatUint(p.planIDsHash, 10) + string(prop.HashCode())
	return p.taskMap[key]
}

// StoreTask records Task for specified property as <k,v>.
func (p *BaseLogicalPlan) StoreTask(prop *property.PhysicalProperty, task base.Task) {
	key := strconv.FormatUint(p.planIDsHash, 10) + string(prop.HashCode())
	if p.SCtx().GetSessionVars().StmtCtx.StmtHints.TaskMapNeedBackUp() {
		// Empty string for useless change.
		ts := p.GetLogicalTS4TaskMap()
		p.taskMapBakTS = append(p.taskMapBakTS, ts)
		p.taskMapBak = append(p.taskMapBak, key)
	}
	p.taskMap[key] = task
}

// ChildLen returns the child length of BaseLogicalPlan.
func (p *BaseLogicalPlan) ChildLen() int {
	return len(p.children)
}

// Self returns the self LogicalPlan of BaseLogicalPlan.
func (p *BaseLogicalPlan) Self() base.LogicalPlan {
	return p.self
}

// SetSelf sets the self LogicalPlan of BaseLogicalPlan.
func (p *BaseLogicalPlan) SetSelf(s base.LogicalPlan) {
	p.self = s
}

// SetFDs sets the FDSet of BaseLogicalPlan.
func (p *BaseLogicalPlan) SetFDs(fd *fd.FDSet) {
	p.fdSet = fd
}

// FDs returns the FDSet of BaseLogicalPlan.
func (p *BaseLogicalPlan) FDs() *fd.FDSet {
	return p.fdSet
}

// SetMaxOneRow sets the maxOneRow of BaseLogicalPlan.
func (p *BaseLogicalPlan) SetMaxOneRow(b bool) {
	p.maxOneRow = b
}

// SetPlanIDsHash set the hash of the subtree rooted from this logical plan.
func (p *BaseLogicalPlan) SetPlanIDsHash(hash uint64) {
	p.planIDsHash = hash
}

// GetPlanIDsHash return the plan ids hash rooted from this logical plan.
func (p *BaseLogicalPlan) GetPlanIDsHash() uint64 {
	return p.planIDsHash
}

// GetWrappedLogicalPlan implements the logical plan interface.
func (p *BaseLogicalPlan) GetWrappedLogicalPlan() base.LogicalPlan {
	return p.self
}

// NewBaseLogicalPlan is the basic constructor of BaseLogicalPlan.
func NewBaseLogicalPlan(ctx base.PlanContext, tp string, self base.LogicalPlan, qbOffset int) BaseLogicalPlan {
	return BaseLogicalPlan{
		taskMap:      make(map[string]base.Task),
		taskMapBak:   make([]string, 0, 10),
		taskMapBakTS: make([]uint64, 0, 10),
		Plan:         baseimpl.NewBasePlan(ctx, tp, qbOffset),
		self:         self,
	}
}

// HasFlag checks if the logical plan has the specified flag.
func (p *BaseLogicalPlan) HasFlag(mask uint64) bool {
	return p.Flag&mask > 0
}

// SetFlag sets the flag for the logical plan.
func (p *BaseLogicalPlan) SetFlag(mask uint64) {
	p.Flag = p.Flag | mask
}

// ReAlloc4Cascades reset some elements in the logical plan.
// those elements shouldn't be shared among different logical plans cuz it will induce unexpected behavior.
// Usage scenario: in the xForm action, the original logical plan in the memo shouldn't be modified.
// while if there is an alternative derived from current logical operator, we should have deep clone one.
func (p *BaseLogicalPlan) ReAlloc4Cascades(tp string, self base.LogicalPlan) {
	// reset the plan inside.
	p.Plan.ReAlloc4Cascades(tp)
	// task map is physical plan memorizing, it shouldn't be shared across different logical operator.
	p.taskMap = make(map[string]base.Task)
	p.taskMapBak = make([]string, 0, 10)
	p.taskMapBakTS = make([]uint64, 0, 10)
	// reset self
	p.self = self
	p.maxOneRow = false
	// keep the children unchanged, unless outer side has some special requirements, do it in the caller.
	// fdSet should be re-derived from the children, cuz apply -> join have different derive logic.
	p.fdSet = nil
}
