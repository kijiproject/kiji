/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.tools.framework;

import java.io.IOException;

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.framework.KijiTableInputJobBuilder;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.ResourceUtils;

/**
 * Base class for tools that run MapReduce jobs over kiji tables.
 *
 * @param <B> The type of job builder to use.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class KijiJobTool<B extends KijiTableInputJobBuilder> extends JobTool<B> {
  @Flag(name="start-row",
      usage="HBase row to start scanning at (inclusive), "
          + "e.g. --start-row='hex:0088deadbeef', or --start-row='utf8:the row key in UTF8'.")
  protected String mStartRowFlag = null;

  @Flag(name="limit-row",
      usage="HBase row to stop scanning at (exclusive), "
          + "e.g. --limit-row='hex:0088deadbeef', or --limit-row='utf8:the row key in UTF8'.")
  protected String mLimitRowFlag = null;

  /** HBase row to start scanning from (inclusive). */
  private byte[] mHBaseStartRow = null;

  /** HBase row to stop scanning at (exclusive). */
  private byte[] mHBaseLimitRow = null;

  /** Job input must be a Kiji table. */
  private KijiTableMapReduceJobInput mJobInput = null;

  /** Creates a new <code>KijiTool</code> instance. */
  protected KijiJobTool() {
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    // Parse --input and --output flags:
    super.validateFlags();

    Preconditions.checkArgument(getJobInput() instanceof KijiTableMapReduceJobInput,
        "Invalid job input '%s' : input must be a Kiji table.", mInputFlag);

    mJobInput = (KijiTableMapReduceJobInput) getJobInput();

    if ((mStartRowFlag != null) && !mStartRowFlag.isEmpty()) {
      mHBaseStartRow = ToolUtils.parseBytesFlag(mStartRowFlag);
    }
    if ((mLimitRowFlag != null) && !mLimitRowFlag.isEmpty()) {
      mHBaseLimitRow = ToolUtils.parseBytesFlag(mLimitRowFlag);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(B jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    // Basic job configuration (base JobConf, jars and KV stores):
    super.configure(jobBuilder);

    // Configure job input:
    jobBuilder.withJobInput(mJobInput);

    final Kiji kiji = Kiji.Factory.open(mJobInput.getInputTableURI());
    try {
      final KijiTable table = kiji.openTable(mJobInput.getInputTableURI().getTable());
      try {
        final EntityIdFactory eidFactory = EntityIdFactory.getFactory(table.getLayout());
        if (mHBaseStartRow != null) {
          jobBuilder.withStartRow(eidFactory.getEntityIdFromHBaseRowKey(mHBaseStartRow));
        }
        if (mHBaseLimitRow != null) {
          jobBuilder.withLimitRow(eidFactory.getEntityIdFromHBaseRowKey(mHBaseLimitRow));
        }
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /** @return the input for this job, which must be a Kiji table. */
  protected KijiTableMapReduceJobInput getJobInputTable() {
    return mJobInput;
  }
}
