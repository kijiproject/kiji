/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.scoring.tools;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;
import org.kiji.scoring.lib.AlwaysFreshen;
import org.kiji.scoring.lib.NeverFreshen;
import org.kiji.scoring.lib.ShelfLife;

public class TestFreshTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFreshTool.class);

  /** Dummy KijiProducer for testing. */
  public static final class TestProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return null;
    }
    public String getOutputColumn() {
      return null;
    }
    public void produce(final KijiRowData input, final ProducerContext context) throws IOException {
    }
  }

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  private String[] mToolOutputLines;

  private int runTool(BaseTool tool, String...arguments) throws Exception {
    mToolOutputBytes.reset();
    final PrintStream pstream = new PrintStream(mToolOutputBytes);
    tool.setPrintStream(pstream);
    tool.setConf(getConf());
    try {
      LOG.info("Running tool: '{}' with parameters {}", tool.getName(), arguments);
      return tool.toolMain(Lists.newArrayList(arguments));
    } finally {
      pstream.flush();
      pstream.close();

      mToolOutputStr = Bytes.toString(mToolOutputBytes.toByteArray());
      LOG.info("Captured output for tool: '{}' with parameters {}:\n{}\n{}{}\n",
          tool.getName(), arguments,
          RULER, mToolOutputStr, RULER);
      mToolOutputLines = mToolOutputStr.split("\n");
    }
  }

  //------------------------------------------------------------------------------------------------

  @Test
  public void testRegister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
        "--do=register",
        "--policy-class=org.kiji.scoring.lib.ShelfLife",
        "--policy-state={\"shelfLife\":10}",
        "--producer-class=org.kiji.scoring.tools.TestFreshTool$TestProducer"
    ));

    final KijiFreshnessPolicyRecord record = KijiFreshnessPolicyRecord.newBuilder()
        .setRecordVersion("policyrecord-0.1.0")
        .setProducerClass(TestProducer.class.getName())
        .setFreshnessPolicyClass(Class.forName("org.kiji.scoring.lib.ShelfLife")
            .asSubclass(KijiFreshnessPolicy.class).getName())
        .setFreshnessPolicyState("{\"shelfLife\":10}")
        .build();
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      assertEquals(record, manager.retrievePolicy("user", "info:name"));
      assertEquals("Freshness policy: org.kiji.scoring.lib.ShelfLife with state: {\"shelfLife\":10}"
          + " and producer: org.kiji.scoring.tools.TestFreshTool$TestProducer\n"
          + "attached to column: info:name in table: user\n", mToolOutputStr);

      // Test another ordering for arguments
      assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
          "--do=register",
          KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
              .withColumnNames(Lists.newArrayList("info:visits")).build().toString(),
          "--policy-state={\"shelfLife\":10}",
          "--producer-class=org.kiji.scoring.tools.TestFreshTool$TestProducer",
          "--policy-class=org.kiji.scoring.lib.ShelfLife"
      ));

      final KijiFreshnessPolicyRecord record2 = KijiFreshnessPolicyRecord.newBuilder()
          .setRecordVersion("policyrecord-0.1.0")
          .setProducerClass(TestProducer.class.getName())
          .setFreshnessPolicyClass(Class.forName("org.kiji.scoring.lib.ShelfLife")
              .asSubclass(KijiFreshnessPolicy.class).getName())
          .setFreshnessPolicyState("{\"shelfLife\":10}")
          .build();
      assertEquals(record2, manager.retrievePolicy("user", "info:visits"));
      assertEquals("Freshness policy: org.kiji.scoring.lib.ShelfLife with state: {\"shelfLife\":10}"
          + " and producer: org.kiji.scoring.tools.TestFreshTool$TestProducer\n"
          + "attached to column: info:visits in table: user\n", mToolOutputStr);
    } finally {
      manager.close();
    }
  }

  @Test
  public void testIllegalRegister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
        "--do=register",
        "--policy-class=org.kiji..badname",
        "--policy-state={\"shelfLife\":10}",
        "--producer-class=org.kiji.scoring.tools.TestFreshTool$TestProducer",
        "--interactive=false");

    assertEquals("BAD_POLICY_NAME: java.lang.IllegalArgumentException: Policy class name: "
        + "org.kiji..badname is not a valid Java class identifier.", mToolOutputLines[0]);


    runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
        "--do=register",
        "--policy-class=org.kiji.class",
        "--policy-state={\"shelfLife\":10}",
        "--producer-class=org.kiji.scoring.tools.TestFreshTool$TestProducer.",
        "--interactive=false");

    assertEquals("BAD_PRODUCER_NAME: java.lang.IllegalArgumentException: Producer class name: org."
        + "kiji.scoring.tools.TestFreshTool$TestProducer. is not a valid Java class identifier.",
        mToolOutputLines[0]);
  }

  @Test
  public void testUnregister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.storePolicy("user", "info:name", TestProducer.class, new NeverFreshen());

      assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
          KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
              .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
          "--do=unregister"
      ));
      assertEquals(null, manager.retrievePolicy("user", "info:name"));
      assertEquals("Freshness policy removed from column: info:name in table user\n",
          mToolOutputStr);
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRetrieve() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.storePolicy("user", "info:name", TestProducer.class, new NeverFreshen());
    } finally {
      manager.close();
    }

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
        "--do=retrieve"
    ));

    assertEquals(
        "Freshness policy class: org.kiji.scoring.lib.NeverFreshen\n"
        + "Freshness policy state: \n"
        + "Producer class: org.kiji.scoring.tools.TestFreshTool$TestProducer\n",
        mToolOutputStr);
  }

  @Test
  public void testRetrieveAll() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.storePolicy("user", "info:name", TestProducer.class, new NeverFreshen());
      manager.storePolicy("user", "info:visits", TestProducer.class, new NeverFreshen());
    } finally {
      manager.close();
    }

    LOG.info(KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build().toString());

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build().toString(),
        "--do=retrieve-all"
    ));

    assertEquals(
        "Freshness policy attached to column: info:visits\n"
        + "  Freshness policy class: org.kiji.scoring.lib.NeverFreshen\n"
        + "  Freshness policy state: \n"
        + "  Producer class: org.kiji.scoring.tools.TestFreshTool$TestProducer\n"
        + "Freshness policy attached to column: info:name\n"
        + "  Freshness policy class: org.kiji.scoring.lib.NeverFreshen\n"
        + "  Freshness policy state: \n"
        + "  Producer class: org.kiji.scoring.tools.TestFreshTool$TestProducer\n",
        mToolOutputStr);
  }

  @Test
  public void testUnregisterAll() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.storePolicy("user", "info:name", TestProducer.class, new NeverFreshen());
      manager.storePolicy("user", "info:visits", TestProducer.class, new NeverFreshen());

      assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
          KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build().toString(),
          "--do=unregister-all"
      ));
      assertEquals(0, manager.retrievePolicies("user").size());
      assertEquals("All freshness policies removed from table: user\n", mToolOutputStr);
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRetrieveEmpty() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .withColumnNames(Lists.newArrayList("info:name")).build().toString(),
        "--do=retrieve"
    ));
    assertEquals("There is no freshness policy attached to column: info:name in table: user\n",
        mToolOutputStr);
  }

  @Test
  public void testRetrieveAllEmpty() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build().toString(),
        "--do=retrieve-all"
    ));
    assertEquals("There are no freshness policies attached to columns in table: user\n",
        mToolOutputStr);
  }

  @Test
  public void testValidate() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.storePolicy("user", "info:name", TestProducer.class, new AlwaysFreshen());
    } finally {
      manager.close();
    }
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build().toString(),
        "--do=validate-all"));

    KijiFreshnessPolicyRecord record = KijiFreshnessPolicyRecord.newBuilder()
        .setRecordVersion(ProtocolVersion.parse("policyrecord-0.1").toCanonicalString())
            .setProducerClass(TestProducer.class.getName())
            .setFreshnessPolicyClass(AlwaysFreshen.class.getName())
            .setFreshnessPolicyState("")
            .build();

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final EncoderFactory encoderFactory = EncoderFactory.get();
    Encoder encoder = encoderFactory.directBinaryEncoder(outputStream, null);
    final DatumWriter<KijiFreshnessPolicyRecord> recordWriter =
        new SpecificDatumWriter<KijiFreshnessPolicyRecord>(KijiFreshnessPolicyRecord.SCHEMA$);
    recordWriter.write(record, encoder);
    getKiji().getMetaTable().putValue("user", "kiji.scoring.fresh.columnName",
        outputStream.toByteArray());

    assertEquals(BaseTool.FAILURE, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build().toString(),
        "--do=validate-all"));
    assertEquals("Freshness policy attached to column: columnName is not valid.",
        mToolOutputLines[1]);
    assertEquals("NO_FAMILY_IN_TABLE: java.lang.IllegalArgumentException: Table: user does not "
        + "contain family: columnName", mToolOutputLines[2]);
  }

  @Test
  public void testStateFromFile() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final String state = new ShelfLife(10L).serialize();
    final File stateFile = File.createTempFile("state", "File", getLocalTempDir());
    final FileWriter fileWriter = new FileWriter(stateFile.getAbsoluteFile());
    final BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    bufferedWriter.write(state);
    bufferedWriter.close();

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        KijiURI.newBuilder(getKiji().getURI()).withTableName("user")
            .addColumnName(new KijiColumnName("info", "name")).build().toString(),
        "--do=register",
        "--policy-class=org.kiji.scoring.lib.ShelfLife",
        "--producer-class=org.kiji.scoring.tools.TestFreshTool$TestProducer",
        "--policy-state-file=" + stateFile.getAbsolutePath()));

    assertEquals("Freshness policy: org.kiji.scoring.lib.ShelfLife with state: {\"shelfLife\":10} "
        + "and producer: org.kiji.scoring.tools.TestFreshTool$TestProducer\nattached to column: "
        + "info:name in table: user\n", mToolOutputStr);
  }
}
