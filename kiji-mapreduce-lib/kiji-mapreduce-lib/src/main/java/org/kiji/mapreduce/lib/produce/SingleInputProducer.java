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

package org.kiji.mapreduce.lib.produce;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;

/**
 * Base class for producers that read from the most recent value of a single input column.
 */
public abstract class SingleInputProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SingleInputProducer.class);

  private KijiColumnName mInputColumn;

  /**
   * @return the name of the kiji input column to feed to this producer.
   */
  protected abstract String getInputColumn();

  /**
   * Initialize the family and qualifier instance variables.
   */
  private void initializeInputColumn() {
    mInputColumn = new KijiColumnName(getInputColumn());
    if (!mInputColumn.isFullyQualified()) {
      throw new RuntimeException("getInputColumn() must contain a colon (':')");
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    initializeInputColumn();
    return KijiDataRequest.create(mInputColumn.getFamily(), mInputColumn.getQualifier());
  }

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) throws IOException {
    initializeInputColumn();
  }

  /** @return the input column family. */
  protected KijiColumnName getInputColumnName() {
    return mInputColumn;
  }
}
