/*
 * ShiftGroupKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A key expression that shifts grouping keys that come after the grouped key so that the entire grouped key is properly on the right.
 * This is needed in cases where a nested concatenated key contains both a grouping key and a grouped key and other grouping keys follow.
 * Since there is no way for the nested {@code concat} expression to include field from the parent, the order needs to be changed afterwards.
 */
@API(API.Status.EXPERIMENTAL)
public class ShiftGroupKeyExpression extends BaseKeyExpression implements KeyExpressionWithoutChildren {
    @Nonnull
    private final KeyExpression wholeKey;
    private final int groupedCount;
    private final int followingGroupingCount;

    public ShiftGroupKeyExpression(@Nonnull KeyExpression wholeKey, int groupedCount, int followingGroupingCount) {
        this.wholeKey = wholeKey;
        this.groupedCount = groupedCount;
        this.followingGroupingCount = followingGroupingCount;
    }

    public ShiftGroupKeyExpression(@Nonnull RecordMetaDataProto.ShiftGroup shiftGroup) throws DeserializationException {
        this(KeyExpression.fromProto(shiftGroup.getWholeKey()), shiftGroup.getGroupedCount(), shiftGroup.getFollowingGroupingCount());
    }

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateMessage(@Nullable FDBRecord<M> record, @Nullable Message message) {
        return getWholeKey().evaluateMessage(record, message).stream()
            .map(v -> Key.Evaluated.concatenate(shift(v.values())))
            .collect(Collectors.toList());
    }

    @Override
    public boolean createsDuplicates() {
        return getWholeKey().createsDuplicates();
    }

    @Override
    public List<Descriptors.FieldDescriptor> validate(@Nonnull Descriptors.Descriptor descriptor) {
        return getWholeKey().validate(descriptor);
    }

    @Override
    public int getColumnSize() {
        return getWholeKey().getColumnSize();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.ShiftGroup toProto() throws SerializationException {
        final RecordMetaDataProto.ShiftGroup.Builder builder = RecordMetaDataProto.ShiftGroup.newBuilder();
        builder.setWholeKey(getWholeKey().toKeyExpression());
        builder.setGroupedCount(groupedCount);
        builder.setFollowingGroupingCount(followingGroupingCount);
        return builder.build();
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.KeyExpression toKeyExpression() {
        return RecordMetaDataProto.KeyExpression.newBuilder().setShiftGroup(toProto()).build();
    }

    @Nonnull
    @Override
    public List<KeyExpression> normalizeKeyForPositions() {
        return shift(getWholeKey().normalizeKeyForPositions());
    }

    @Nonnull
    @Override
    public KeyExpression normalizeForPlanner(@Nonnull Source source, @Nonnull List<String> fieldNamePrefix) {
        return new ShiftGroupKeyExpression(wholeKey.normalizeForPlanner(source, fieldNamePrefix), groupedCount, followingGroupingCount);
    }

    @Override
    public int versionColumns() {
        return getWholeKey().versionColumns();
    }

    @Override
    public boolean hasRecordTypeKey() {
        return getWholeKey().hasRecordTypeKey();
    }

    @Nonnull
    public KeyExpression getWholeKey() {
        return wholeKey;
    }

    public int getGroupedCount() {
        return groupedCount;
    }

    public int getFollowingGroupingCount() {
        return followingGroupingCount;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(getWholeKey().toString());
        str.append(" shift ").append(followingGroupingCount).append(" over ").append(groupedCount);
        return str.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ShiftGroupKeyExpression that = (ShiftGroupKeyExpression)o;
        return this.getWholeKey().equals(that.getWholeKey()) &&
            (this.groupedCount == that.groupedCount) &&
            (this.followingGroupingCount == that.followingGroupingCount);
    }

    @Override
    public int hashCode() {
        int hash = getWholeKey().hashCode();
        hash += groupedCount;
        hash += followingGroupingCount;
        return hash;
    }

    @Override
    public int planHash() {
        return getWholeKey().planHash() + groupedCount + followingGroupingCount;
    }

    private <T> List<T> shift(@Nonnull List<T> unshifted) {
        List<T> shifted = new ArrayList<>(unshifted.size());
        shifted.addAll(unshifted.subList(0, unshifted.size() - groupedCount - followingGroupingCount));
        shifted.addAll(unshifted.subList(unshifted.size() - followingGroupingCount, unshifted.size()));
        shifted.addAll(unshifted.subList(unshifted.size() - groupedCount - followingGroupingCount, unshifted.size() - followingGroupingCount));
        return shifted;
    }
}
