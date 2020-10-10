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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The scd2 plugin
 */
@Name("SCD2")
@Plugin(type = SparkCompute.PLUGIN_TYPE)
public class SCD2Plugin extends SparkCompute<StructuredRecord, StructuredRecord> {
  private final Conf conf;

  public SCD2Plugin(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    conf.validate(stageConfigurer.getInputSchema(), failureCollector);
    failureCollector.getOrThrowException();
    stageConfigurer.setOutputSchema(conf.getOutputSchema(stageConfigurer.getInputSchema()));
  }

  @Override
  public void prepareRun(SparkPluginContext context) {
    conf.validate(context.getInputSchema(), context.getFailureCollector());
    List<FieldOperation> ops = new ArrayList<FieldOperation>();

    // Fill in basic transformations
    ops.add(new FieldTransformOperation(conf.key + " SCD2", "copy", Collections.singletonList(conf.key), conf.key));
    ops.add(new FieldTransformOperation(
      conf.startDateField + " SCD2", "copy", Collections.singletonList(conf.startDateField),
      conf.startDateField));
    ops.add(new FieldTransformOperation(
      conf.endDateField + " SCD2", "Computed end date field from the start date field",
      Collections.singletonList(conf.startDateField), conf.endDateField));

    // Description of the general transformation
    String desc = "copy";
    if (conf.fillInNull()) {
      desc = desc + ", fill from previous if empty";
    }
    if (conf.deduplicate()) {
      desc = desc + ", remove duplicate rows";
    }

    Schema outputSchema = conf.getOutputSchema(context.getInputSchema());
    if (outputSchema == null) {
      context.record(ops);
      return;
    }

    // Fill in general transforms
    for (Schema.Field field : outputSchema.getFields()) {
      String fname = field.getName();
      if (fname.equals(conf.startDateField) || fname.equals(conf.endDateField) || fname.equals(conf.key)) {
        continue;
      }
      ops.add(new FieldTransformOperation(fname + " SCD2", desc, Collections.singletonList(fname), fname));
    }
    // Record the lineage
    context.record(ops);
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext context,
                                             JavaRDD<StructuredRecord> javaRDD) {
    return new SCD2Processor(conf).process(javaRDD);
  }

  /**
   * Conf for scd2 plugin
   */
  @SuppressWarnings({"unused", "ConstantConditions"})
  public static class Conf extends PluginConfig {
    private static final String KEY = "key";
    private static final String START_DATE_FIELD = "startDateField";
    private static final String END_DATE_FIELD = "endDateField";
    private static final String PLACEHOLDER = "placeHolderFields";
    private static final String BLACKLIST = "blacklist";
    private static final String OFFSET = "endDateOffset";
    private static final String PRESERVE_TARGET = "preserveTarget";
    private static final String IS_TARGET_FIELD = "isTargetField";

    @Macro
    @Description("The name of the key field. The records will be grouped based on the key. " +
                   "This field must be comparable.")
    private String key;

    @Macro
    @Description("The name of the start date field. The grouped records are sorted based on this field.")
    private String startDateField;

    @Macro
    @Description("The name of the end date field. The sorted results are iterated to compute the value of this " +
                   "field based on the start date.")
    private String endDateField;

    @Nullable
    @Macro
    @Description("The offset to compute the end date. The end date will be computed as the next start date minus " +
                   "this offset. The format is expected to be a number followed by an 'us', 'ms', 's', 'm', 'h', " +
                   "or 'd' specifying the time unit, with 'us' for microseconds, 'ms' for milliseconds, 's' for " +
                   "seconds, 'm' for minutes, 'h' for hours, and 'd' for days. By default, the offset is 1us.")
    private String endDateOffset;

    @Nullable
    @Macro
    @Description("Deduplicate records that have no changes.")
    private Boolean deduplicate;

    @Nullable
    @Macro
    @Description("Preserve all the records from the scd2 target table when deduplicating.")
    private Boolean preserveTarget;

    @Nullable
    @Macro
    @Description("The name of the field that tells whether the record is coming from the scd2 target table. " +
                   "This field must be boolean.")
    private String isTargetField;

    @Nullable
    @Macro
    @Description("Blacklist for fields to ignore to compare when deduplicating the record.")
    private String blacklist;

    @Nullable
    @Macro
    @Description("Fill in null fields from most recent previous record.")
    private Boolean fillInNull;

    @Nullable
    @Macro
    @Description("Number of partitions to use when grouping fields. If not specified, the execution" +
                   "framework will decide on the number to use.")
    private Integer numPartitions;

    @VisibleForTesting
    public Conf(String key, String startDateField, String endDateField, boolean deduplicate,
                boolean fillInNull, String blacklist, boolean preserveTarget, String isTargetField) {
      this.key = key;
      this.startDateField = startDateField;
      this.endDateField = endDateField;
      this.deduplicate = deduplicate;
      this.fillInNull = fillInNull;
      this.blacklist = blacklist;
      this.preserveTarget = preserveTarget;
      this.isTargetField = isTargetField;
    }

    public String getKey() {
      return key;
    }

    public String getStartDateField() {
      return startDateField;
    }

    public String getEndDateField() {
      return endDateField;
    }

    public long getEndDateOffset() {
      return TimeParser.parseDuration(endDateOffset == null ? "1us" : endDateOffset);
    }

    public boolean deduplicate() {
      return deduplicate == null ? false : deduplicate;
    }

    public boolean preserveTarget() {
      return preserveTarget == null ? false : preserveTarget;
    }

    public String getIsTargetField() {
      return isTargetField;
    }

    public boolean fillInNull() {
      return fillInNull == null ? false : fillInNull;
    }

    public int getNumPartitions() {
      return numPartitions == null ? 200 : numPartitions;
    }

    public Set<String> getBlacklist() {
      return getFields(BLACKLIST, blacklist);
    }

    private Set<String> getFields(String fieldName, String fieldString) {
      Set<String> fields = new HashSet<>();
      if (containsMacro(fieldName) || fieldString == null) {
        return fields;
      }
      for (String field : Splitter.on(',').trimResults().split(fieldString)) {
        fields.add(field);
      }
      return fields;
    }

    private void validate(@Nullable Schema actualSchema, FailureCollector failureCollector) {
      if (actualSchema == null) {
        return;
      }

      if (!containsMacro(KEY)) {
        Schema.Field field = actualSchema.getField(key);
        if (field == null) {
          failureCollector.addFailure(String.format("The %s field '%s' does not exist in input schema.", KEY, key),
                                      null).withConfigElement(KEY, key);
        } else {
          Schema schema = field.getSchema();
          Schema.Type fieldType = schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
          if (!schema.isSimpleOrNullableSimple()) {
            failureCollector.addFailure(String.format("The %s field '%s' must be a boolean, int, long, " +
                                                        "float, double, bytes or string type in " +
                                                        "the input schema.", KEY, key), null)
              .withConfigElement(KEY, key);
          }
        }
      }

      if (!containsMacro(START_DATE_FIELD)) {
        Schema.Field field = actualSchema.getField(startDateField);
        if (field == null) {
          failureCollector.addFailure(String.format("The %s field '%s' does not exist in input schema.",
                                                    START_DATE_FIELD, startDateField),
                                      null).withConfigElement(START_DATE_FIELD, startDateField);
        } else {
          Schema schema = field.getSchema();
          Schema.LogicalType fieldType = schema.isNullable() ? schema.getNonNullable().getLogicalType() :
                                           schema.getLogicalType();
          if (!Schema.LogicalType.TIMESTAMP_MICROS.equals(fieldType)) {
            failureCollector.addFailure(String.format("The %s field '%s' is not timestamp type in " +
                                                        "the input schema.", START_DATE_FIELD, startDateField), null)
              .withConfigElement(START_DATE_FIELD, startDateField);
          }
        }
      }

      if (!containsMacro(END_DATE_FIELD)) {
        Schema.Field field = actualSchema.getField(endDateField);
        if (field != null) {
          Schema schema = field.getSchema();
          Schema.LogicalType fieldType = schema.isNullable() ? schema.getNonNullable().getLogicalType() :
                                           schema.getLogicalType();
          if (!Schema.LogicalType.TIMESTAMP_MICROS.equals(fieldType)) {
            failureCollector.addFailure(String.format("The %s field '%s' is not timestamp type in " +
                                                        "the input schema.", END_DATE_FIELD, endDateField), null)
              .withConfigElement(END_DATE_FIELD, endDateField);
          }
        }
      }

      if (!containsMacro(OFFSET) && getEndDateOffset() < 0) {
        failureCollector.addFailure(String.format("The %s field has value %d, it must not be a negative number",
                                                  OFFSET, getEndDateOffset()), null);
      }

      if (!containsMacro(PRESERVE_TARGET) && !containsMacro(IS_TARGET_FIELD)) {
        if (preserveTarget() && isTargetField == null) {
          failureCollector.addFailure(
            String.format("The %s field must be specified if preserve target is set to true.",
                          IS_TARGET_FIELD), null);
        }

        if (isTargetField != null) {
          Schema.Field field = actualSchema.getField(isTargetField);
          if (field == null) {
            failureCollector.addFailure(String.format("The %s field '%s' does not exist in input schema.",
                                                      IS_TARGET_FIELD, isTargetField),
                                        null).withConfigElement(IS_TARGET_FIELD, isTargetField);
          } else {
            Schema schema = field.getSchema();
            if (schema.isNullable()) {
              failureCollector.addFailure(String.format("The %s field '%s' is must not be nullable in " +
                                                          "the input schema.", IS_TARGET_FIELD, isTargetField), null)
                .withConfigElement(IS_TARGET_FIELD, isTargetField);
            } else if (!Schema.Type.BOOLEAN.equals(schema.getType())) {
              failureCollector.addFailure(String.format("The %s field '%s' is not boolean type in " +
                                                          "the input schema.", IS_TARGET_FIELD, isTargetField), null)
                .withConfigElement(IS_TARGET_FIELD, isTargetField);
            }
          }
        }
      }
    }

    @Nullable
    public Schema getOutputSchema(@Nullable Schema inputSchema) {
      if (inputSchema == null) {
        return null;
      }

      if (!containsMacro(END_DATE_FIELD) && inputSchema.getField(endDateField) != null) {
        return inputSchema;
      }

      List<Schema.Field> fields = new ArrayList<>(inputSchema.getFields());
      Schema endSchema = !containsMacro(START_DATE_FIELD) ? inputSchema.getField(startDateField).getSchema() :
                           Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      fields.add(Schema.Field.of(endDateField, endSchema));
      return Schema.recordOf(inputSchema.getRecordName(), fields);
    }
  }
}
