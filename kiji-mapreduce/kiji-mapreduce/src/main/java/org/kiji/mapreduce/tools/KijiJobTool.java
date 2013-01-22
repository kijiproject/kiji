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

package org.kiji.mapreduce.tools;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiTableInputJobBuilder;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.KijiTable;
import org.kiji.schema.tools.ToolUtils;

/**
 * Base class for tools that run MapReduce jobs over kiji tables.
 *
 * @param <B> The type of job builder to use.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class KijiJobTool<B extends KijiTableInputJobBuilder> extends JobTool<B> {
  @Flag(name="start-row", usage="The row to start scanning at (inclusive)")
  protected String mStartRow = "";

  @Flag(name="limit-row", usage="The row to stop scanning at (exclusive)")
  protected String mLimitRow = "";

  /** The kiji table. */
  private KijiTable mKijiTable;

  /** Creates a new <code>KijiTool</code> instance. */
  protected KijiJobTool() {
    mKijiTable = null;
  }

  @Override
  protected void validateFlags() throws Exception {
    super.validateFlags();
    if (mInputSpec.getFormat() != JobInputSpec.Format.KIJI) {
      throw new RuntimeException("--input must be 'kiji:<tablename>'");
    }
  }

  @Override
  protected void setup() throws Exception {
    super.setup();
    assert 1 == mInputSpec.getLocations().length; // This is enforced by the JobInputSpec c'tor.
    mKijiTable = getKiji().openTable(mInputSpec.getLocation());
  }

  @Override
  protected void cleanup() throws IOException {
    if (null != mKijiTable) {
      mKijiTable.close();
    }
    super.cleanup();
  }

  @Override
  protected void configure(B jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    super.configure(jobBuilder);
    jobBuilder.withInputTable(mKijiTable);
    final EntityIdFactory eidFactory = mKijiTable.getEntityIdFactory();
    if (!mStartRow.isEmpty()) {
      jobBuilder.withStartRow(eidFactory.fromHBaseRowKey(ToolUtils.parseRowKeyFlag(mStartRow)));
    }
    if (!mLimitRow.isEmpty()) {
      jobBuilder.withLimitRow(eidFactory.fromHBaseRowKey(ToolUtils.parseRowKeyFlag(mLimitRow)));
    }
  }

  /**
   * Gets the Kiji table this job will run over.
   *
   * @return The input Kiji table.
   */
  protected KijiTable getInputTable() {
    return mKijiTable;
  }
}
