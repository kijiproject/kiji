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
import org.kiji.schema.EntityId;
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
      usage="Entity ID of the row to start scanning at (inclusive).\n"
          + "\tEither 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'.\n"
          + ("\tHBase row keys are specified as bytes:\n"
              + "\t\tby default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring';\n"
              + "\t\tin hexadecimal as in 'hex:deadbeef';\n"
              + "\t\tas a URL with 'url:this%20URL%00'.\n")
          + "\tOld deprecated Kiji row keys are specified as naked UTF-8 strings.\n"
          + ("\tNew Kiji row keys are specified in JSON, "
              + "as in: --start-row=kiji=\"['component1', 2, 'component3']\"."))
  protected String mStartRowFlag = null;

  @Flag(name="limit-row",
      usage="Entity ID of the row to stop scanning at (exclusive).\n"
          + "\tEither 'kiji=<Kiji row key>' or 'hbase=<HBase row key>'.\n"
          + ("\tHBase row keys are specified as bytes:\n"
              + "\t\tby default as UTF-8 strings, or prefixed as in 'utf8:encoded\\x0astring';\n"
              + "\t\tin hexadecimal as in 'hex:deadbeef';\n"
              + "\t\tas a URL with 'url:this%20URL%00'.\n")
          + "\tOld deprecated Kiji row keys are specified as naked UTF-8 strings.\n"
          + ("\tNew Kiji row keys are specified in JSON, "
              + "as in: --limit-row=kiji=\"['component1', 2, 'component3']\"."))
  protected String mLimitRowFlag = null;

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
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(B jobBuilder)
      throws ClassNotFoundException, IOException {
    // Basic job configuration (base JobConf, jars and KV stores):
    super.configure(jobBuilder);

    // Configure job input:
    jobBuilder.withJobInput(mJobInput);

    final Kiji kiji = Kiji.Factory.open(mJobInput.getInputTableURI());
    try {
      final KijiTable table = kiji.openTable(mJobInput.getInputTableURI().getTable());
      try {
        if (mStartRowFlag != null) {
          final EntityId eidLimit =
              ToolUtils.createEntityIdFromUserInputs(mStartRowFlag, table.getLayout());
          jobBuilder.withStartRow(eidLimit);
        }
        if (mLimitRowFlag != null) {
          final EntityId eidLimit =
              ToolUtils.createEntityIdFromUserInputs(mLimitRowFlag, table.getLayout());
          jobBuilder.withLimitRow(eidLimit);
        }
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /** @return the input for this job, which must be a Kiji table. */
  protected final KijiTableMapReduceJobInput getJobInputTable() {
    return mJobInput;
  }
}
