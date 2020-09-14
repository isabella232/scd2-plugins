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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * The scd2 processor. This class is used to mitigate the validation issue.
 */
public final class SCD2Processor {
  private final SCD2Plugin.Conf conf;

  public SCD2Processor(SCD2Plugin.Conf conf) {
    this.conf = conf;
  }

  JavaRDD<StructuredRecord> process(JavaRDD<StructuredRecord> javaRDD) {
    return javaRDD.mapToPair(new RecordToKeyRecordPairFunction(conf.getKey(), conf.getStartDateField()))
             .repartitionAndSortWithinPartitions(new HashPartitioner(conf.getNumPartitions()),
                                                 new KeyComparator())
             // records are now sorted by key and start date (desc). ex: r1, r2, r3, r4
             // we need to walk the records in order and update the end time of r2 to be start time of r1 - 1.
             .mapPartitions(new SCD2FlatMapFunction(conf));
  }

  /**
   * Compare the scd2key, first compare the key and then compare the start date.
   */
  public static class KeyComparator implements Comparator<SCD2Key>, Serializable {

    @Override
    public int compare(SCD2Key k1, SCD2Key k2) {
      Comparable k1Key = k1.getKey();
      Comparable k2Key = k2.getKey();
      if (k1Key == k2Key && k1Key == null) {
        return 0;
      }

      if (k1Key == null) {
        return -1;
      }

      if (k2Key == null) {
        return 1;
      }

      int cmp = k1Key.compareTo(k2Key);
      if (cmp != 0) {
        return cmp;
      }

      return Long.compare(k1.getStartDate(), k2.getStartDate());
    }
  }
}
