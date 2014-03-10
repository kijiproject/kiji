/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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
package org.kiji.scoring.lib;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.PrintWriter;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.Resources;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;

/**
 * Tests for JpmmlScoreFunction. Currently only tests one model given the assumption that JPMML has
 * tested the logic of its implemented evaluators.
 */
public class TestJpmmlScoreFunction extends KijiClientTest {
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupTestInternalFreshKijiTableReader() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout simpleLinearRegressionLayout =
        KijiTableLayouts.getTableLayout("simple-linear-regression.json");

    // Populate the environment.
    final Schema predictorSchema =
        Schema.createRecord("SimpleLinearRegressionPredictor", null, null, false);
    predictorSchema.setFields(
        Lists.newArrayList(
            new Schema.Field("predictor", Schema.create(Schema.Type.DOUBLE), null, null)
        )
    );
    final GenericRecord predictor = new GenericRecordBuilder(predictorSchema)
        .set("predictor", 2.0)
        .build();
    new InstanceBuilder(getKiji())
        .withTable(simpleLinearRegressionLayout.getName(), simpleLinearRegressionLayout)
            .withRow("foo").withFamily("model").withQualifier("predictor").withValue(predictor)
        .build();

    // Fill local variables.
    mTable = getKiji().openTable(simpleLinearRegressionLayout.getName());
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupTestInternalFreshKijiTableReader() throws Exception {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testLinearRegressionModel() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("model", "predicted");

    // Write model to a temporary file.
    final String pmmlModelString =
        IOUtils.toString(Resources.openSystemResource("simple-linear-regression.xml"));
    final File pmmlModelFile = File.createTempFile(
        TestJpmmlScoreFunction.class.getName() + "-simple-linear-regression",
        ".xml"
    );
    pmmlModelFile.deleteOnExit();
    final PrintWriter pmmlPrintWriter = new PrintWriter(pmmlModelFile);
    try {
      pmmlPrintWriter.println(pmmlModelString);
    } finally {
      pmmlPrintWriter.flush();
      pmmlPrintWriter.close();
    }

    // Setup parameters for score function.
    final Map<String, Schema> predicted = Maps.newHashMap();
    predicted.put("predicted", Schema.create(Schema.Type.DOUBLE));
    final Map<String, String> parameters = JpmmlScoreFunction.parameters(
        "file://" + pmmlModelFile.getAbsolutePath(),
        "linearreg",
        new KijiColumnName("model", "predictor"),
        "SimpleLinearRegressionPredicted",
        predicted
    );

    // Create a KijiFreshnessManager and register some Fresheners.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          "linearreg",
          new KijiColumnName("model", "predicted"),
          new AlwaysFreshen(),
          new JpmmlScoreFunction(),
          parameters,
          false,
          false
      );
    } finally {
      manager.close();
    }
    // Open a new reader to pull in the new freshness policies. Allow 10 seconds so it is very
    // unlikely to timeout.
    final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
        .withTable(mTable)
        .withTimeout(10000)
        .build();
    try {
      final GenericRecord result =
          freshReader.get(eid, request).getMostRecentValue("model", "predicted");
      // Ensure score is correct.
      assertEquals(
          5.0,
          result.get("predicted")
      );
    } finally {
      freshReader.close();
    }
  }
}
