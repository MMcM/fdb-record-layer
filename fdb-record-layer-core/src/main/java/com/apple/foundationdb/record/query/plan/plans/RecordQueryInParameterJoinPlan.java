/*
 * RecordQueryInParameterJoinPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A query plan that executes a child plan once for each of the elements of an {@code IN} list taken from a parameter.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryInParameterJoinPlan extends RecordQueryInJoinPlan {
    private final String externalBinding;

    public RecordQueryInParameterJoinPlan(RecordQueryPlan plan, String bindingName, String externalBinding, boolean sortValues, boolean sortReverse) {
        super(plan, bindingName, sortValues, sortReverse);
        this.externalBinding = externalBinding;
    }

    @Override
    @Nullable
    @SuppressWarnings("unchecked")
    protected List<Object> getValues(EvaluationContext context) {
        return sortValues((List)context.getBinding(externalBinding));
    }

    public String getExternalBinding() {
        return externalBinding;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getInner().toString());
        str.append(" WHERE ").append(bindingName)
                .append(" IN $").append(externalBinding);
        if (sortValuesNeeded) {
            str.append(" SORTED");
            if (sortReverse) {
                str.append(" DESC");
            }
        }
        return str.toString();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return otherExpression instanceof RecordQueryInParameterJoinPlan &&
               super.equalsWithoutChildren(otherExpression) &&
               externalBinding.equals(((RecordQueryInParameterJoinPlan)otherExpression).externalBinding);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RecordQueryInParameterJoinPlan that = (RecordQueryInParameterJoinPlan) o;
        return Objects.equals(externalBinding, that.externalBinding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), externalBinding);
    }

    @Override
    public int planHash() {
        return super.planHash() + externalBinding.hashCode();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_IN_PARAMETER);
        getInner().logPlanStructure(timer);
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models this operator as a logical nested loop join
     *         joining an outer table of iterated values over a parameter in the IN clause to the correlated inner
     *         result of executing (usually) a index lookup for each bound outer value.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR);
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.NodeWithInfo explodeNode =
                new PlannerGraph.LogicalOperatorNodeWithInfo(NodeInfo.TABLE_FUNCTION_OPERATOR,
                        ImmutableList.of("EXPLODE({{externalBinding}})"),
                        ImmutableMap.of("externalBinding", Attribute.gml(externalBinding)));
        final PlannerGraph.Edge fromExplodeEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(explodeNode)
                .addEdge(explodeNode, root, fromExplodeEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromExplodeEdge)))
                .build();
    }
}
