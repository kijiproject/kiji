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

package org.kiji.mapreduce.produce.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.KijiProducerOutputException;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;

/** Utility methods for working with <code>KijiProducer</code>s. */
@ApiAudience.Private
public final class KijiProducers {
  /** Disable the constructor for this utility class. */
  private KijiProducers() {}

  /**
   * Creates an instance of the producer specified by the
   * {@link org.apache.hadoop.conf.Configuration}.
   *
   * <p>The configuration would have stored the producer name only if
   * it was configured by a KijiProduceJobBuilder, so don't try calling this
   * method with any old Configuration object.</p>
   *
   * @param conf The job configuration.
   * @return a brand-spankin'-new KijiProducer instance.
   * @throws IOException If the producer name cannot be instantiated from the configuration.
   */
  public static KijiProducer create(Configuration conf) throws IOException {
    final Class<? extends KijiProducer> producerClass =
        conf.getClass(KijiConfKeys.KIJI_PRODUCER_CLASS, null, KijiProducer.class);
    if (null == producerClass) {
      throw new IOException("Producer class could not be found in configuration.");
    }
    return ReflectionUtils.newInstance(producerClass, conf);
  }

  /**
   * Makes sure the producer's requested output column exists in the
   * kiji table layout.
   *
   * @param producer The producer whose output column should be validated.
   * @param tableLayout The layout of the table to validate the output column against.
   * @throws KijiProducerOutputException If the output column cannot be written to.
   */
  public static void validateOutputColumn(KijiProducer producer, KijiTableLayout tableLayout)
      throws KijiProducerOutputException {
    final String outputColumn = producer.getOutputColumn();
    if (null == outputColumn) {
      throw new KijiProducerOutputException(String.format(
          "Producer '%s' must specify an output column by overridding getOutputColumn().",
          producer.getClass().getName()));
    }

    final KijiColumnName columnName = new KijiColumnName(outputColumn);
    final FamilyLayout family = tableLayout.getFamilyMap().get(columnName.getFamily());
    if (null == family) {
      throw new KijiProducerOutputException(String.format(
          "Producer '%s' specifies unknown output column family '%s' in table '%s'.",
          producer.getClass().getName(), columnName.getFamily(), tableLayout.getName()));
    }

    // When writing to a particular column qualifier of a group, make sure it exists:
    if (columnName.isFullyQualified()
        && family.isGroupType()
        && !family.getColumnMap().containsKey(columnName.getQualifier())) {
      throw new KijiProducerOutputException(String.format(
          "Producer '%s' specifies unknown column '%s' in table '%s'.",
          producer.getClass().getName(), columnName, tableLayout.getName()));
    }
  }
}
