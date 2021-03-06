/*
 * SyntheticRecordFromStoredRecordPlan.java
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

package com.apple.foundationdb.record.query.plan.synthetic;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBSyntheticRecord;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A plan for deriving synthetic records from a given record.
 *
 * Like {@link SyntheticRecordPlan}, but with the additional {@code execute}-time parameter of a seed record.
 * While the former is used to generate all records to rebuild an index, this plan is used to generate affected
 * records for a changed record to update just for those changes.
 *
 */
@API(API.Status.INTERNAL)
public interface SyntheticRecordFromStoredRecordPlan extends PlanHashable  {
    /**
     * Get the possible record types for the stored record to which this plan can be applied.
     *
     * If given a record whose type is not in this set, the plan may return an empty cursor or throw an exception.
     * @return the set of record type names
     */
    @Nonnull
    Set<String> getStoredRecordTypes();

    /**
     * Get the possible record types for the synthetic record generated by this plan.
     *
     * @return the set of record type names
     */
    @Nonnull
    Set<String> getSyntheticRecordTypes();

    /**
     * Execute this plan.
     * @param store record store against which to execute
     * @param record the stored record from which to derive synthetic records, such as by join queries
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @param <M> type of raw record
     * @return a cursor of synthetic records
     */
    @Nonnull
    <M extends Message> RecordCursor<FDBSyntheticRecord> execute(@Nonnull FDBRecordStore store,
                                                                 @Nonnull FDBStoredRecord<M> record,
                                                                 @Nullable byte[] continuation,
                                                                 @Nonnull ExecuteProperties executeProperties);

    /**
     * Execute this plan.
     * @param store record store against which to execute
     * @param record the stored record from which to derive synthetic records, such as by join queries
     * @param <M> type of raw record
     * @return a cursor of synthetic records
     */
    @Nonnull
    default <M extends Message> RecordCursor<FDBSyntheticRecord> execute(@Nonnull FDBRecordStore store,
                                                                         @Nonnull FDBStoredRecord<M> record) {
        return execute(store, record, null, ExecuteProperties.SERIAL_EXECUTE);
    }

}
