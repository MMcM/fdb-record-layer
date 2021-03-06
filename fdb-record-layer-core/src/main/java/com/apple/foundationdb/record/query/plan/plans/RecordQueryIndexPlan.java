/*
 * RecordQueryIndexPlan.java
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that outputs records pointed to by entries in a secondary index within some range.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryIndexPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithComparisons, RecordQueryPlanWithIndex, PlannerGraphRewritable {
    @Nonnull
    protected final String indexName;
    @Nonnull
    protected final IndexScanType scanType;
    @Nonnull
    protected final ScanComparisons comparisons;
    protected final boolean reverse;

    public RecordQueryIndexPlan(@Nonnull final String indexName, @Nonnull IndexScanType scanType, @Nonnull final ScanComparisons comparisons, final boolean reverse) {
        this.indexName = indexName;
        this.scanType = scanType;
        this.comparisons = comparisons;
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                       @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties) {
        final TupleRange range = comparisons.toTupleRange(store, context);
        final RecordMetaData metaData = store.getRecordMetaData();
        return store.scanIndex(metaData.getIndex(indexName), scanType, range, continuation, executeProperties.asScanProperties(reverse));
    }

    @Nonnull
    @Override
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    @Override
    public ScanComparisons getComparisons() {
        return comparisons;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanType;
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return this.indexName.equals(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return Collections.singleton(indexName);
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression) {
        return equals(otherExpression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordQueryIndexPlan)) {
            return false;
        }
        RecordQueryIndexPlan that = (RecordQueryIndexPlan) o;
        return reverse == that.reverse &&
                Objects.equals(indexName, that.indexName) &&
                Objects.equals(scanType, that.scanType) &&
                Objects.equals(comparisons, that.comparisons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, scanType, comparisons, reverse);
    }

    @Override
    public int planHash() {
        return indexName.hashCode() + scanType.planHash() + comparisons.planHash() + (reverse ? 1 : 0);
    }

    @Nonnull
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("Index(");
        appendScanDetails(str);
        str.append(")");
        return str.toString();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INDEX);
    }

    protected void appendScanDetails(StringBuilder str) {
        @Nullable final TupleRange tupleRange = comparisons.toTupleRangeWithoutContext();
        final String range = tupleRange == null ? comparisons.toString() : tupleRange.toString();
        str.append(indexName).append(" ").append(range);
        if (scanType != IndexScanType.BY_VALUE) {
            str.append(" ").append(scanType);
        }
        if (reverse) {
            str.append(" REVERSE");
        }
    }


    @Override
    public int getComplexity() {
        return 1;
    }

    /**
     * Create a planner graph for better visualization of a query index plan.
     * @return the rewritten planner graph that models the index as a separate node that is connected to the
     *         actual index scan plan node.
     */
    @Nonnull
    @Override
    public PlannerGraph createIndexPlannerGraph(@Nonnull RecordQueryPlan identity,
                                                @Nonnull final NodeInfo nodeInfo,
                                                @Nonnull final List<String> additionalDetails,
                                                @Nonnull final Map<String, Attribute> additionalAttributeMap) {
        @Nullable final TupleRange tupleRange = comparisons.toTupleRangeWithoutContext();

        final ImmutableList.Builder<String> detailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> attributeMapBuilder = ImmutableMap.builder();

        detailsBuilder
                .addAll(additionalDetails);
        attributeMapBuilder
                .putAll(additionalAttributeMap);

        if (scanType != IndexScanType.BY_VALUE) {
            detailsBuilder.add("scan type: {{scanType}}");
            attributeMapBuilder.put("scanType", Attribute.gml(scanType.toString()));
        }

        if (tupleRange != null) {
            detailsBuilder.add("range: " + tupleRange.getLowEndpoint().toString(false) + "{{low}}, {{high}}" + tupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("low", Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
            attributeMapBuilder.put("high", Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("comparisons: {{comparisons}}");
            attributeMapBuilder.put("comparisons", Attribute.gml(comparisons.toString()));
        }

        if (reverse) {
            detailsBuilder.add("direction: {{direction}}");
            attributeMapBuilder.put("direction", Attribute.gml("reversed"));
        }

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(identity,
                        nodeInfo,
                        detailsBuilder.build(),
                        attributeMapBuilder.build()),
                ImmutableList.of(
                        PlannerGraph.fromNodeAndChildGraphs(
                                new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, ImmutableList.copyOf(getUsedIndexes())),
                                ImmutableList.of())));
    }
}
