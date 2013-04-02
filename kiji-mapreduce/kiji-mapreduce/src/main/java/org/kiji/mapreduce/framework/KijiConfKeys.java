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

package org.kiji.mapreduce.framework;

import org.kiji.annotations.ApiAudience;

/** Configuration keys used by KijiMR in Hadoop Configuration objects. */
@ApiAudience.Framework
public final class KijiConfKeys {

  /** URI of the input table to read from. */
  public static final String KIJI_INPUT_TABLE_URI = "kiji.input.table.uri";

  /** URI of the output Kiji table to write to. */
  public static final String KIJI_OUTPUT_TABLE_URI = "kiji.output.table.uri";

  /** Name of the KijiTableContext class to use. */
  public static final String KIJI_TABLE_CONTEXT_CLASS = "kiji.table.context.class";

  /** Serialized input data request. */
  public static final String KIJI_INPUT_DATA_REQUEST = "kiji.input.data.request";

  /** Fully qualified name of the KijiGatherer class to run. */
  public static final String KIJI_GATHERER_CLASS = "kiji.gatherer.class";

  /** Fully qualified name of the KijiProducer class to run. */
  public static final String KIJI_PRODUCER_CLASS = "kiji.producer.class";

  /** Fully qualified name of the KijiBulkImporter class to run. */
  public static final String KIJI_BULK_IMPORTER_CLASS = "kiji.bulk.importer.class";

  /** Kiji Instance Name. */
  public static final String KIJI_INSTANCE_NAME = "kiji.instance.name";

  /** Polling interval in milliseconds for Kiji MapReduce jobs. */
  public static final String KIJI_MAPREDUCE_POLL_INTERVAL = "kiji.mapreduce.poll.interval";

  /** Utility class may not be instantiated. */
  private KijiConfKeys() {
  }
}
