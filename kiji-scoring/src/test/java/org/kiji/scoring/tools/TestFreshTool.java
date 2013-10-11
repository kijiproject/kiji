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

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.lib.NeverFreshen;
import org.kiji.scoring.lib.ShelfLife;

public class TestFreshTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestFreshTool.class);

  private static final class TestScoreFunction extends ScoreFunction {
    public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
      return null;
    }
    public Object score(
        final KijiRowData dataToScore, final FreshenerContext context
    ) throws IOException {
      return null;
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

  private static final KijiColumnName INFO_NAME = new KijiColumnName("info", "name");
  private static final KijiColumnName INFO_VISITS = new KijiColumnName("info", "visits");
  private static final String SHELF_LIFE_STRING = "org.kiji.scoring.lib.ShelfLife";
  private static final String SCORE_FN_STRING =
      "org.kiji.scoring.tools.TestFreshTool$TestScoreFunction";
  private static final String POLICY_STRING = "org.kiji.scoring.lib.NeverFreshen";
  private static final Map<String, String> PARAMS = Maps.newHashMap();
  static {
    PARAMS.put(ShelfLife.SHELF_LIFE_KEY, "10");
  }
  private static final Map<String, String> EMPTY_PARAMS = Collections.emptyMap();
  private static final String PARAMS_STRING =
      String.format("{\"%s\":\"10\"}", ShelfLife.SHELF_LIFE_KEY);
  private static final String EMPTY_PARAMS_STRING = "{}";
  private static final KijiFreshnessPolicy NEVER = new NeverFreshen();
  private static final ScoreFunction SCORE_FN = new TestScoreFunction();

  private KijiURI getUserURI() throws IOException {
    return KijiURI.newBuilder(getKiji().getURI()).withTableName("user").build();
  }

  private KijiURI getNameURI() throws IOException {
    return KijiURI.newBuilder(getKiji().getURI())
        .withTableName("user").withColumnNames(Lists.newArrayList(INFO_NAME)).build();
  }

  private KijiURI getVisitsURI() throws IOException {
    return KijiURI.newBuilder(getKiji().getURI())
        .withTableName("user").withColumnNames(Lists.newArrayList(INFO_VISITS)).build();
  }

  @Test
  public void testRegister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        "--target=" + getNameURI(),
        "--do=register",
        "--policy-class=" + SHELF_LIFE_STRING,
        "--score-function-class=" + SCORE_FN_STRING,
        "--parameters=" + PARAMS_STRING
    ));

    final KijiFreshenerRecord record = KijiFreshenerRecord.newBuilder()
        .setRecordVersion(KijiFreshnessManager.CUR_FRESHENER_RECORD_VER.toCanonicalString())
        .setFreshnessPolicyClass(SHELF_LIFE_STRING)
        .setScoreFunctionClass(SCORE_FN_STRING)
        .setParameters(PARAMS)
        .build();
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      assertEquals(record, manager.retrieveFreshenerRecord("user", INFO_NAME));

      // Test another ordering for arguments
      assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
          "--do=register",
          "--target=" + getVisitsURI(),
          "--policy-class=" + SHELF_LIFE_STRING,
          "--parameters=" + PARAMS_STRING,
          "--score-function-class=" + SCORE_FN_STRING
      ));

      assertEquals(record, manager.retrieveFreshenerRecord("user", INFO_VISITS));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testIllegalRegister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    assertEquals(BaseTool.FAILURE, runTool(new FreshTool(),
        "--target=" + getNameURI(),
        "--do=register",
        "--policy-class=org.kiji..badname",
        "--score-function-class=" + SCORE_FN_STRING));

    assertEquals("There were validation failures in column: info:name", mToolOutputLines[0]);
    assertEquals("  BAD_POLICY_NAME: KijiFreshnessPolicy class name is not a valid Java identifier."
        + " Found: org.kiji..badname", mToolOutputLines[1]);


    assertEquals(BaseTool.FAILURE, runTool(new FreshTool(),
        "--target=" + getNameURI(),
        "--do=register",
        "--policy-class=org.kiji.class",
        "--score-function-class=org.kiji.scoring.tools.TestFreshTool$TestScoreFunction."));

    assertEquals("There were validation failures in column: info:name", mToolOutputLines[0]);
    assertEquals("  BAD_SCORE_FUNCTION_NAME: ScoreFunction class name is not a valid Java "
        + "identifier. Found: org.kiji.scoring.tools.TestFreshTool$TestScoreFunction.",
        mToolOutputLines[1]);
  }

  @Test
  public void testUnregister() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener(
          "user", INFO_NAME, NEVER, SCORE_FN, EMPTY_PARAMS, false, false);

      assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
          "--target=" + getNameURI(),
          "--do=remove"
      ));
      assertNull(manager.retrieveFreshenerRecord("user", INFO_NAME));
      assertEquals("Fresheners removed from:\n  info:name\n",
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
      manager.registerFreshener(
          "user", INFO_NAME, NEVER, SCORE_FN, EMPTY_PARAMS, false, false);
    } finally {
      manager.close();
    }

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        "--target=" + getNameURI(),
        "--do=retrieve"
    ));

    assertEquals("Freshener attached to column: 'info:name' in table: 'user'\n"
        + "  KijiFreshnessPolicy class: 'org.kiji.scoring.lib.NeverFreshen'\n"
        + "  ScoreFunction class: 'org.kiji.scoring.tools.TestFreshTool$TestScoreFunction'\n"
        + "  Parameters: {}\n",
        mToolOutputStr);
  }

  @Test
  public void testRetrieveAll() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener("user", INFO_NAME, NEVER, SCORE_FN, EMPTY_PARAMS, false, false);
      manager.registerFreshener("user", INFO_VISITS, NEVER, SCORE_FN, EMPTY_PARAMS, false, false);
    } finally {
      manager.close();
    }

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        "--target=" + getUserURI(),
        "--do=retrieve"));

    final String nameOutput = String.format("Freshener attached to column: '%s' in table: '%s'%n"
        + "  KijiFreshnessPolicy class: '%s'%n"
        + "  ScoreFunction class: '%s'%n"
        + "  Parameters: %s%n",
        INFO_NAME, "user", POLICY_STRING, SCORE_FN_STRING, EMPTY_PARAMS_STRING);

    final String visitsOutput = String.format("Freshener attached to column: '%s' in table: '%s'%n"
        + "  KijiFreshnessPolicy class: '%s'%n"
        + "  ScoreFunction class: '%s'%n"
        + "  Parameters: %s%n",
        INFO_VISITS, "user", POLICY_STRING, SCORE_FN_STRING, EMPTY_PARAMS_STRING);

    assertTrue(mToolOutputStr.contains(nameOutput) && mToolOutputStr.contains(visitsOutput));
  }

  @Test
  public void testUnregisterAll() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      manager.registerFreshener("user", INFO_NAME, NEVER, SCORE_FN, EMPTY_PARAMS, false, false);
      manager.registerFreshener("user", INFO_VISITS, NEVER, SCORE_FN, EMPTY_PARAMS, false, false);

      assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
          "--target=" + getUserURI(),
          "--do=remove"
      ));
      assertTrue(manager.retrieveFreshenerRecords("user").isEmpty());
      assertEquals("Fresheners removed from:", mToolOutputLines[0]);
      assertTrue(mToolOutputStr.contains(String.format("  %s%n", INFO_NAME))
              && mToolOutputStr.contains(String.format("  %s%n", INFO_VISITS)));
    } finally {
      manager.close();
    }
  }

  @Test
  public void testRetrieveEmpty() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        "--target=" + getNameURI(),
        "--do=retrieve"
    ));
    assertEquals("No Freshener attached to column: 'info:name' in table: 'user'\n",
        mToolOutputStr);
  }

  @Test
  public void testRetrieveAllEmpty() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        "--target=" + getUserURI(),
        "--do=retrieve"
    ));
    assertEquals("No Fresheners attached to columns in table: 'user'\n",
        mToolOutputStr);
  }

  @Test
  public void testOptionalParameters() throws Exception {
    getKiji().createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    assertEquals(BaseTool.SUCCESS, runTool(new FreshTool(),
        "--target=" + getNameURI(),
        "--do=register",
        "--policy-class=" + POLICY_STRING,
        "--score-function-class=" + SCORE_FN_STRING));

    final KijiFreshenerRecord record = KijiFreshenerRecord.newBuilder()
        .setRecordVersion(KijiFreshnessManager.CUR_FRESHENER_RECORD_VER.toCanonicalString())
        .setFreshnessPolicyClass(POLICY_STRING)
        .setScoreFunctionClass(SCORE_FN_STRING)
        .setParameters(EMPTY_PARAMS)
        .build();

    final KijiFreshnessManager manager = KijiFreshnessManager.create(getKiji());
    try {
      assertEquals(record, manager.retrieveFreshenerRecord("user", INFO_NAME));
    } finally {
      manager.close();
    }
  }
}
