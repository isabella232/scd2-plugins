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

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Test for SCD2 plugin
 */
public class SCD2PluginTest extends HydratorTestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private static final ArtifactSummary APP_ARTIFACT_PIPELINE =
    new ArtifactSummary("data-pipeline", "1.0.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifactPipeline =
      NamespaceId.DEFAULT.artifact(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion());

    setupBatchArtifacts(parentArtifactPipeline, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact("scd2-plugins", "1.0.0"),
                      parentArtifactPipeline, SCD2Plugin.class);
  }

  @Test
  public void testSCD2() throws Exception {
    Schema schema = Schema.recordOf(
      "x",
      Schema.Field.of("id", Schema.of(Schema.Type.INT)),
      Schema.Field.of("startDate", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("endDate", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    Map<String, String> properties = new HashMap<>();
    properties.put("key", "id");
    properties.put("startDateField", "startDate");
    properties.put("endDateField", "endDate");
    properties.put("numPartitions", "1");
    String inputDataset = UUID.randomUUID().toString();
    String outputDateset = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder()
                              .addStage(new ETLStage("source", MockSource.getPlugin(inputDataset, schema)))
                              .addStage(new ETLStage("scd2", new ETLPlugin("SCD2",
                                                                           SparkCompute.PLUGIN_TYPE, properties)))
                              .addStage(new ETLStage("sink", MockSink.getPlugin(outputDateset)))
                              .addConnection("source", "scd2")
                              .addConnection("scd2", "sink")
                              .build();


    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(APP_ARTIFACT_PIPELINE.getName(), APP_ARTIFACT_PIPELINE.getVersion()), config);
    ApplicationId appId = NamespaceId.DEFAULT.app("SCD2");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    List<StructuredRecord> input = new ArrayList<>();
    input.add(StructuredRecord.builder(schema).set("id", 0).set("startDate", 0L).set("endDate", 10000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 1).set("startDate", 10000000L)
                .set("endDate", 20000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 0)
                .set("startDate", 1000000000L).set("endDate", 5000000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 1)
                .set("startDate", 21000000L).set("endDate", 1000000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 0).set("startDate", 100000000L).build());
    input.add(StructuredRecord.builder(schema).set("id", 1).set("startDate", 15000000L).build());
    DataSetManager<Table> inputManager = getDataset(inputDataset);
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDateset);
    List<StructuredRecord> output = MockSink.readOutput(outputManager);
    output.sort((r1, r2) -> {
      int id1 = r1.get("id");
      int id2 = r2.get("id");
      int cmp = Integer.compare(id1, id2);
      if (cmp != 0) {
        return cmp;
      }

      long sdate1 = r1.get("startDate");
      long sdate2 = r2.get("startDate");
      return Long.compare(sdate1, sdate2);
    });

    List<StructuredRecord> expected = new ArrayList<>();
    expected.add(StructuredRecord.builder(schema).set("id", 0).set("startDate", 0L).set("endDate", 99999999L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("startDate", 100000000L).set("endDate", 999999999L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 0)
                   .set("startDate", 1000000000L).set("endDate", 253402214400000000L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("startDate", 10000000L).set("endDate", 14999999L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("startDate", 15000000L).set("endDate", 20999999L).build());
    expected.add(StructuredRecord.builder(schema).set("id", 1)
                   .set("startDate", 21000000L).set("endDate", 253402214400000000L).build());

    Assert.assertEquals(expected, output);
  }
}
