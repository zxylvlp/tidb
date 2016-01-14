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

package plan

import (
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
)

// Join is the struct for join plan.
type Join struct {
	planWithSrc

	table     *ast.TableName
	equiv     *Equiv
	Condition ast.ExprNode
}

// Accept implements Plan interface.
func (p *Join) Accept(v Visitor) (Plan, bool) {
	np, skip := v.Enter(p)
	if skip {
		v.Leave(np)
	}
	p = np.(*Join)
	if p.src != nil {
		var ok bool
		p.src, ok = p.src.Accept(v)
		if !ok {
			return p, false
		}
	}
	return v.Leave(p)
}

// Equiv represents a equivalent join condition.
type Equiv struct {
	Left  *ast.ResultField
	Right *ast.ResultField
}

type joinPath struct {
	table           *ast.TableName
	equiv           *Equiv
	conditions      []ast.ExprNode
	filterRate      float64
	totalFilterRate float64
	outerDeps       map[*joinPath]struct{}
	ordering        *ast.ResultField
	orderingDesc    bool
}

// attachConditions attaches input conditions to self if applicable, returns remaining conditions.
func (p *joinPath) attachConditions(allConditions []ast.ExprNode) (remained []ast.ExprNode) {
	// TODO:
	return allConditions
}

func (p *joinPath) computeFilterRate() {
	// TODO:
}

// optimizeJoin builds an optimized plan for join.
func optimizeJoin(sel *ast.SelectStmt) (Plan, error) {
	from := sel.From.TableRefs
	if from.Right == nil {
		return singleTablePlan(from, sel.Where)
	}
	paths := buildPaths(from)
	rfs := buildFieldsFromPaths(paths)
	conditions := append(extractConditions(from), splitWhere(sel.Where)...)
	for i := 0; i < len(paths); i++ {
		conditions = paths[i].attachConditions(conditions)
		paths[i].computeFilterRate()
	}
	equivs := extractEquivs(conditions)
	optimizedPaths := optimizeJoinPaths(paths, equivs)
	planA := planFromPath(optimizedPaths)
	planA.SetFields(rfs)
	err := Refine(planA)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if matchOrder(planA, sel) {
		return planA, nil
	}

	orderedPaths := orderedPaths(paths, equivs, sel.GroupBy, sel.OrderBy)
	if orderedPaths == nil {
		return planA, nil
	}
	planB := planFromPath(orderedPaths)
	err = Refine(planB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	costA := EstimateCost(pseudoPlan(planA, false, sel))
	costB := EstimateCost(pseudoPlan(planB, true, sel))
	if costA < costB {
		return planA, nil
	}
	return planB, nil
}

func matchOrder(joinPlan *Join, sel *ast.SelectStmt) bool {
	// TODO:
	return false
}

// pseudoPlan pre-build complete plan only for cost estimation.
func pseudoPlan(p Plan, ordered bool, sel *ast.SelectStmt) Plan {
	// TODO:
	return nil
}

func singleTablePlan(from *ast.Join, where ast.ExprNode) (Plan, error) {
	ts, ok := from.Left.(*ast.TableSource)
	if !ok {
		return nil, ErrUnsupportedType.Gen("Unsupported type %T", from.Left)
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, ErrUnsupportedType.Gen("Unsupported type %T", ts.Source)
	}
	p := &TableScan{
		Table:  tn.TableInfo,
		Ranges: []TableRange{{math.MinInt64, math.MaxInt64}},
	}
	p.SetFields(tn.GetResultFields())
	if where != nil {
		filter := &Filter{
			Conditions: splitWhere(where),
		}
		filter.SetSrc(p)
		filter.SetFields(p.Fields())
		return filter, nil
	}
	return p, nil
}

// extractConditions extracts conditions from *ast.Join element.
func extractConditions(from *ast.Join) []ast.ExprNode {
	// TODO:
	return nil
}

// buildPaths builds original ordered paths and set outer dependencies.
func buildPaths(from *ast.Join) []*joinPath {
	// TODO:
	return nil
}

// buildFieldsFromPaths builds result field from original ordered paths.
func buildFieldsFromPaths(paths []*joinPath) []*ast.ResultField {
	var rfs []*ast.ResultField
	for _, path := range paths {
		rfs = append(rfs, path.table.GetResultFields()...)
	}
	return rfs
}

// optimizePaths computes an optimal join order.
func optimizeJoinPaths(paths []*joinPath, equivs []*Equiv) []*joinPath {
	// TODO:
	return nil
}

// extractEquivs extracts equivalence expression like 't1.c1 = t2.c1',
// used to optimize join order.
func extractEquivs(conditions []ast.ExprNode) []*Equiv {
	// TODO:
	return nil
}

// planFromPath creates Join plan from paths.
func planFromPath(paths []*joinPath) *Join {
	// TODO:
	return nil
}

// orderedPaths tries to build ordered paths according to group by and order by.
// so the sort can be avoid.
func orderedPaths(paths []*joinPath, equivs []*Equiv,
	groupBy *ast.GroupByClause, orderBy *ast.OrderByClause) []*joinPath {
	// TODO:
	return nil
}
