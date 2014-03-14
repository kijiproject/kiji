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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.IOUtils;
import org.dmg.pmml.DataType;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.joda.time.Seconds;
import org.jpmml.evaluator.DaysSinceDate;
import org.jpmml.evaluator.SecondsSinceDate;
import org.jpmml.evaluator.SecondsSinceMidnight;
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
  public static final LocalDate EXAMPLE_DATE_JPMML = new LocalDate(2013, 1, 2);
  public static final String EXAMPLE_DATE_AVRO = EXAMPLE_DATE_JPMML.toString();
  public static final LocalTime EXAMPLE_TIME_JPMML = new LocalTime(15, 38, 50);
  public static final String EXAMPLE_TIME_AVRO = EXAMPLE_TIME_JPMML.toString();
  public static final LocalDateTime EXAMPLE_DATE_TIME_JPMML =
      EXAMPLE_DATE_JPMML.toLocalDateTime(EXAMPLE_TIME_JPMML);
  public static final String EXAMPLE_DATE_TIME_AVRO = EXAMPLE_DATE_TIME_JPMML.toString();

  public static final DaysSinceDate EXAMPLE_DAYS_SINCE_0_JPMML =
      new DaysSinceDate(JpmmlScoreFunction.DATE_YEAR_0, EXAMPLE_DATE_JPMML);
  public static final DaysSinceDate EXAMPLE_DAYS_SINCE_1960_JPMML =
      new DaysSinceDate(JpmmlScoreFunction.DATE_YEAR_1960, EXAMPLE_DATE_JPMML);
  public static final DaysSinceDate EXAMPLE_DAYS_SINCE_1970_JPMML =
      new DaysSinceDate(JpmmlScoreFunction.DATE_YEAR_1970, EXAMPLE_DATE_JPMML);
  public static final DaysSinceDate EXAMPLE_DAYS_SINCE_1980_JPMML =
      new DaysSinceDate(JpmmlScoreFunction.DATE_YEAR_1980, EXAMPLE_DATE_JPMML);
  public static final int EXAMPLE_DAYS_SINCE_0_AVRO = EXAMPLE_DAYS_SINCE_0_JPMML.intValue();
  public static final int EXAMPLE_DAYS_SINCE_1960_AVRO = EXAMPLE_DAYS_SINCE_1960_JPMML.intValue();
  public static final int EXAMPLE_DAYS_SINCE_1970_AVRO = EXAMPLE_DAYS_SINCE_1970_JPMML.intValue();
  public static final int EXAMPLE_DAYS_SINCE_1980_AVRO = EXAMPLE_DAYS_SINCE_1980_JPMML.intValue();

  public static final SecondsSinceMidnight EXAMPLE_SECONDS_SINCE_MIDNIGHT_JPMML =
      new SecondsSinceMidnight(Seconds.seconds(42));
  public static final int EXAMPLE_SECONDS_SINCE_MIDNIGHT_AVRO =
      EXAMPLE_SECONDS_SINCE_MIDNIGHT_JPMML.getSeconds().getSeconds();

  public static final SecondsSinceDate EXAMPLE_SECONDS_SINCE_0_JPMML =
      new SecondsSinceDate(JpmmlScoreFunction.DATE_YEAR_0, new LocalDateTime(10, 1, 1, 0, 0, 0));
  public static final SecondsSinceDate EXAMPLE_SECONDS_SINCE_1960_JPMML =
      new SecondsSinceDate(JpmmlScoreFunction.DATE_YEAR_1960, EXAMPLE_DATE_TIME_JPMML);
  public static final SecondsSinceDate EXAMPLE_SECONDS_SINCE_1970_JPMML =
      new SecondsSinceDate(JpmmlScoreFunction.DATE_YEAR_1970, EXAMPLE_DATE_TIME_JPMML);
  public static final SecondsSinceDate EXAMPLE_SECONDS_SINCE_1980_JPMML =
      new SecondsSinceDate(JpmmlScoreFunction.DATE_YEAR_1980, EXAMPLE_DATE_TIME_JPMML);
  public static final int EXAMPLE_SECONDS_SINCE_0_AVRO =
      EXAMPLE_SECONDS_SINCE_0_JPMML.intValue();
  public static final int EXAMPLE_SECONDS_SINCE_1960_AVRO =
      EXAMPLE_SECONDS_SINCE_1960_JPMML.intValue();
  public static final int EXAMPLE_SECONDS_SINCE_1970_AVRO =
      EXAMPLE_SECONDS_SINCE_1970_JPMML.intValue();
  public static final int EXAMPLE_SECONDS_SINCE_1980_AVRO =
      EXAMPLE_SECONDS_SINCE_1980_JPMML.intValue();

  private KijiTable mTable;
  private KijiTableReader mReader;

  public void testAvroDataTypeConversion(
      final Object data,
      final DataType dataType
  ) throws Exception {
    final Object converted = JpmmlScoreFunction.convertAvroToFieldValue(data, dataType);
    assertEquals(data, JpmmlScoreFunction.convertFieldValueToAvro(converted, dataType));
  }

  public void testJpmmlDataTypeConversion(
      final Object data,
      final DataType dataType
  ) throws Exception {
    final Object converted = JpmmlScoreFunction.convertFieldValueToAvro(data, dataType);
    assertEquals(data, JpmmlScoreFunction.convertAvroToFieldValue(converted, dataType));
  }

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
  public void testIntDataTypeConversion() throws Exception {
    testAvroDataTypeConversion(42, DataType.INTEGER);
    testJpmmlDataTypeConversion(42, DataType.INTEGER);
  }

  @Test
  public void testStringDataTypeConversions() throws Exception {
    testAvroDataTypeConversion("foo", DataType.STRING);
    testJpmmlDataTypeConversion("foo", DataType.STRING);
  }

  @Test
  public void testFloatDataTypeConversion() throws Exception {
    testAvroDataTypeConversion(3.14f, DataType.FLOAT);
    testJpmmlDataTypeConversion(3.14f, DataType.FLOAT);
  }

  @Test
  public void testDoubleDataTypeConversion() throws Exception {
    testAvroDataTypeConversion(3.14, DataType.DOUBLE);
    testJpmmlDataTypeConversion(3.14, DataType.DOUBLE);
  }

  @Test
  public void testBooleanDataTypeConversion() throws Exception {
    testAvroDataTypeConversion(true, DataType.BOOLEAN);
    testJpmmlDataTypeConversion(true, DataType.BOOLEAN);
  }

  @Test
  public void testDateDataTypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_DATE_AVRO, DataType.DATE);
    testJpmmlDataTypeConversion(EXAMPLE_DATE_JPMML, DataType.DATE);
  }

  @Test
  public void testTimeDataTypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_TIME_AVRO, DataType.TIME);
    testJpmmlDataTypeConversion(EXAMPLE_TIME_JPMML, DataType.TIME);
  }

  @Test
  public void testDateTimeDateTypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_DATE_TIME_AVRO, DataType.DATE_TIME);
    testJpmmlDataTypeConversion(EXAMPLE_DATE_TIME_JPMML, DataType.DATE_TIME);
  }

  @Test
  public void testDaysSince0TypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_DAYS_SINCE_0_AVRO, DataType.DATE_DAYS_SINCE_0);
    testJpmmlDataTypeConversion(EXAMPLE_DAYS_SINCE_0_JPMML, DataType.DATE_DAYS_SINCE_0);
  }

  @Test
  public void testDaysSince1960TypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_DAYS_SINCE_1960_AVRO, DataType.DATE_DAYS_SINCE_1960);
    testJpmmlDataTypeConversion(EXAMPLE_DAYS_SINCE_1960_JPMML, DataType.DATE_DAYS_SINCE_1960);
  }

  @Test
  public void testDaysSince1970TypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_DAYS_SINCE_1970_AVRO, DataType.DATE_DAYS_SINCE_1970);
    testJpmmlDataTypeConversion(EXAMPLE_DAYS_SINCE_1970_JPMML, DataType.DATE_DAYS_SINCE_1970);
  }

  @Test
  public void testDaysSince1980TypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_DAYS_SINCE_1980_AVRO, DataType.DATE_DAYS_SINCE_1980);
    testJpmmlDataTypeConversion(EXAMPLE_DAYS_SINCE_1980_JPMML, DataType.DATE_DAYS_SINCE_1980);
  }

  @Test
  public void testSecondsSinceMidnightTypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_SECONDS_SINCE_MIDNIGHT_AVRO, DataType.TIME_SECONDS);
    testJpmmlDataTypeConversion(EXAMPLE_SECONDS_SINCE_MIDNIGHT_JPMML, DataType.TIME_SECONDS);
  }

  @Test
  public void testSecondsSince0TypeConversion() throws Exception {
    testAvroDataTypeConversion(EXAMPLE_SECONDS_SINCE_0_AVRO, DataType.DATE_TIME_SECONDS_SINCE_0);
    testJpmmlDataTypeConversion(EXAMPLE_SECONDS_SINCE_0_JPMML, DataType.DATE_TIME_SECONDS_SINCE_0);
  }

  @Test
  public void testSecondsSince1960TypeConversion() throws Exception {
    testAvroDataTypeConversion(
        EXAMPLE_SECONDS_SINCE_1960_AVRO,
        DataType.DATE_TIME_SECONDS_SINCE_1960
    );
    testJpmmlDataTypeConversion(
        EXAMPLE_SECONDS_SINCE_1960_JPMML,
        DataType.DATE_TIME_SECONDS_SINCE_1960
    );
  }

  @Test
  public void testSecondsSince1970TypeConversion() throws Exception {
    testAvroDataTypeConversion(
        EXAMPLE_SECONDS_SINCE_1970_AVRO,
        DataType.DATE_TIME_SECONDS_SINCE_1970
    );
    testJpmmlDataTypeConversion(
        EXAMPLE_SECONDS_SINCE_1970_JPMML,
        DataType.DATE_TIME_SECONDS_SINCE_1970
    );
  }

  @Test
  public void testSecondsSince1980TypeConversion() throws Exception {
    testAvroDataTypeConversion(
        EXAMPLE_SECONDS_SINCE_1980_AVRO,
        DataType.DATE_TIME_SECONDS_SINCE_1980
    );
    testJpmmlDataTypeConversion(
        EXAMPLE_SECONDS_SINCE_1980_JPMML,
        DataType.DATE_TIME_SECONDS_SINCE_1980
    );
  }

  @Test
  public void testLinearRegressionModel() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("model", "predicted");

    // Write model to a temporary file.
    final File pmmlModelFile = File.createTempFile(
        TestJpmmlScoreFunction.class.getName() + "-simple-linear-regression",
        ".xml"
    );
    {
      final String pmmlModelString =
          IOUtils.toString(Resources.openSystemResource("simple-linear-regression.xml"));
      pmmlModelFile.deleteOnExit();
      final PrintWriter pmmlPrintWriter = new PrintWriter(pmmlModelFile);
      try {
        pmmlPrintWriter.println(pmmlModelString);
      } finally {
        pmmlPrintWriter.flush();
        pmmlPrintWriter.close();
      }
    }

    // Setup parameters for score function.
    final Map<String, String> parameters = JpmmlScoreFunction.parameters(
        "file://" + pmmlModelFile.getAbsolutePath(),
        "linearreg",
        new KijiColumnName("model", "predictor"),
        "SimpleLinearRegressionPredicted"
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
