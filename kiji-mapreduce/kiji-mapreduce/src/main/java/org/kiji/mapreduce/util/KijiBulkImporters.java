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

package org.kiji.mapreduce.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.mapreduce.KijiBulkImporter;

/** Utility methods for working with <code>KijiBulkImporter</code>. */
public final class KijiBulkImporters {
  /** Utility class. */
  private KijiBulkImporters() {}

  /**
   * <p>Create an instance of the bulk importer specified by the
   * {@link org.apache.hadoop.conf.Configuration}.</p>
   *
   * The configuration would have stored the bulk importer
   * name only if it was configured by a KijiBulkImportJob, so don't try
   * calling this method with any old Configuration object.
   *
   * @param <K> The map input key for the bulk importer.
   * @param <V> The map input value for the bulk importer.
   * @param conf The job configuration.
   * @return a brand-spankin'-new KijiBulkImporter instance.
   * @throws IOException If the bulk importer cannot be loaded.
   */
  @SuppressWarnings("unchecked")
  public static <K, V> KijiBulkImporter<K, V> create(Configuration conf) throws IOException {
    Class<? extends KijiBulkImporter> bulkImporterClass
        = conf.getClass(KijiBulkImporter.CONF_BULK_IMPORTER_CLASS, null, KijiBulkImporter.class);
    if (null == bulkImporterClass) {
      throw new IOException("Unable to load bulk importer class");
    }

    return ReflectionUtils.newInstance(bulkImporterClass, conf);
  }
}
