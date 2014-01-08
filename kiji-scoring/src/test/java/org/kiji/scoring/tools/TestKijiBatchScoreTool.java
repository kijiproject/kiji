package org.kiji.scoring.tools;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.util.InstanceBuilder;

public class TestKijiBatchScoreTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiBatchScoreTool.class);

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

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupTestScoreFunctionJobBuilder() throws IOException {
    mKiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST))
            .withRow("foo")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(5L, "foo-val")
            .withRow("bar")
                .withFamily("family")
                    .withQualifier("qual0")
                        .withValue(5L, "bar-val")
        .build();
    mTable = mKiji.openTable("row_data_test_table");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupTestScoreFunctionJobBuilder() throws IOException {
    mReader.close();
    mTable.release();
  }

  private KijiURI getTableURI() throws IOException {
    return KijiURI.newBuilder(getKiji().getURI()).withTableName("row_data_test_table").build();
  }

  @Test
  public void testSimpleJob() throws Exception {
    assertEquals(BaseTool.SUCCESS, runTool(new KijiBatchScoreTool(),
        String.format("--input=format=kiji table=%s", getTableURI()),
        String.format("--output=format=kiji table=%s nsplits=1", getTableURI()),
        "--score-function-class=org.kiji.scoring.impl.TestInternalFreshKijiTableReader$TestScoreFunction",
        "--attached-column=family:qual0",
        "--interactive=false"
    ));

    final EntityId fooId = mTable.getEntityId("foo");
    final EntityId barId = mTable.getEntityId("bar");
    final KijiDataRequest request = KijiDataRequest.create("family", "qual0");
    assertEquals("new-val",
        mReader.get(fooId, request).getMostRecentValue("family", "qual0").toString());
    assertEquals("new-val",
        mReader.get(barId, request).getMostRecentValue("family", "qual0").toString());
  }
}
