/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.scd2;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Table;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The scd2 iterator, it keeps track of cur, prev, next from the given iterator.
 */
public class SCD2Iterator extends AbstractIterator<StructuredRecord> {
  // 9999-12-31 00:00:00 timestamp in micro seconds
  private static final long ACTIVE_TS = 253402214400000000L;
  private final PeekingIterator<Tuple2<SCD2Key, StructuredRecord>> records;
  private final Table<Object, String, Object> valTable;
  private final SCD2Plugin.Conf conf;
  private final Set<String> blacklist;
  private Schema outputSchema;
  private Tuple2<SCD2Key, StructuredRecord> cur;
  private Tuple2<SCD2Key, StructuredRecord> prev;
  private Tuple2<SCD2Key, StructuredRecord> next;

  public SCD2Iterator(Iterator<Tuple2<SCD2Key, StructuredRecord>> records, SCD2Plugin.Conf conf) {
    this.records = Iterators.peekingIterator(records);
    this.conf = conf;
    this.blacklist = conf.getBlacklist();
    this.valTable = HashBasedTable.create();
  }

  @Override
  protected StructuredRecord computeNext() {
    // if the records does not have value, but next still have a value, we still need to process it
    if (!records.hasNext() && next == null) {
      return endOfData();
    }

    prev = cur;
    if (prev == null) {
      cur = deduplicateNext();
    } else {
      cur = next;
    }
    next = records.hasNext() ? deduplicateNext() : null;

    // if key changes, clean up the table to free memory
    if (prev != null && !prev._1().equals(cur._1())) {
      valTable.row(prev._1()).clear();
    }

    return computeRecord(cur._1(),
                         prev != null && prev._1().equals(cur._1()) ? prev._2() : null,
                         cur._2(),
                         next != null && next._1().equals(cur._1()) ? next._2() : null);
  }

  /**
   * Find the next record that will not get deduplicated and get rid of the other duplicated records.
   *
   * For example, consider the following example:
   * 1. id: 1, name: john, isTarget: false
   * 2. id: 1, name: john, isTarget: true
   * 3. id: 1, name: john, isTarget: false
   * 4. id: 1, name: sam, isTarget: false
   * 5. id: 1, name: sam, isTarget: false
   *
   * After this method, the records are processed like this:
   * 1. id: 1, name: john, isTarget: false (get ignored since target is false and it is same as #2)
   * 2. id: 1, name: john, isTarget: true (will be returned since target is true)
   * 3. id: 1, name: john, isTarget: false (get ignored since target is false and it is same as #2)
   * 4. id: 1, name: sam, isTarget: false (iterator.peek() now points to it since it is a candiate for the
   *                                       next non-dedup record)
   * 5. id: 1, name: sam, isTarget: false (will not get processed in the method)
   */
  private Tuple2<SCD2Key, StructuredRecord> deduplicateNext() {
    Tuple2<SCD2Key, StructuredRecord> currentElement = records.next();
    Tuple2<SCD2Key, StructuredRecord> result = currentElement;
    boolean findResult = false;

    while (conf.deduplicate() && records.hasNext()) {
      Tuple2<SCD2Key, StructuredRecord> nextElement = records.peek();
      if (isTarget(currentElement._2())) {
        findResult = true;
      }

      // if the current is different from next or if the next is from target table, we can stop deduplicating
      if (isDiff(currentElement._2(), nextElement._2()) || isTarget(nextElement._2())) {
        break;
      }

      // by default, dedup will find latest start date
      currentElement = records.next();
      result = !findResult ? currentElement : result;
    }

    return result;
  }

  /**
   * Return if the current record is from the target table
   */
  private boolean isTarget(StructuredRecord record) {
    return conf.preserveTarget() && (Boolean) record.get(conf.getIsTargetField());
  }

  /**
   * Check if the two records are different
   */
  private boolean isDiff(StructuredRecord cur, StructuredRecord next) {
    for (Schema.Field field : cur.getSchema().getFields()) {
      String fieldName = field.getName();
      if (blacklist.contains(fieldName)) {
        continue;
      }

      // check if there is difference between next record and cur record
      if (!Objects.equals(cur.get(fieldName), next.get(fieldName))) {
        return true;
      }
    }
    return false;
  }

  private StructuredRecord computeRecord(SCD2Key key, @Nullable StructuredRecord prev, StructuredRecord cur,
                                         @Nullable StructuredRecord next) {
    if (outputSchema == null) {
      outputSchema = conf.getOutputSchema(cur.getSchema());
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);

    for (Schema.Field field : cur.getSchema().getFields()) {
      String fieldName = field.getName();
      Object value = cur.get(fieldName);

      // fill in null from previous record
      if (conf.fillInNull() && value == null) {
        value = valTable.get(key, fieldName);
      }
      builder.set(fieldName, value);
      if (conf.fillInNull() && value != null) {
        valTable.put(key, fieldName, value);
      }
    }

    long endDate;
    if (next == null) {
      endDate = ACTIVE_TS;
    } else {
      Long date = next.get(conf.getStartDateField());
      endDate = date == null ? ACTIVE_TS : date - conf.getEndDateOffset();
    }
    builder.set(conf.getEndDateField(), endDate);
    return builder.build();
  }
}
