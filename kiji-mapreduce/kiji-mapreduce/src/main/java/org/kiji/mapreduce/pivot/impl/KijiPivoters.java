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

package org.kiji.mapreduce.pivot.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.pivot.KijiPivoter;

/** Utility methods for working with {@link KijiPivoter}. */
@ApiAudience.Private
public final class KijiPivoters {
  /** Utility class. */
  private KijiPivoters() {}

  /**
   * Create an instance of {@link KijiPivoter} as specified from a given
   * {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param conf The job configuration.
   * @return a new {@link KijiPivoter} instance.
   * @throws IOException if the class cannot be loaded.
   */
  public static KijiPivoter create(Configuration conf) throws IOException {
    final Class<? extends KijiPivoter> tableMapperClass =
        conf.getClass(KijiConfKeys.KIJI_PIVOTER_CLASS, null, KijiPivoter.class);
    if (null == tableMapperClass) {
      throw new IOException("Unable to load pivoter class");
    }
    return ReflectionUtils.newInstance(tableMapperClass, conf);
  }

  /**
   * Loads a {@link KijiPivoter} class by name.
   *
   * @param className Fully qualified name of the class to load.
   * @return the loaded class.
   * @throws ClassNotFoundException if the class is not found.
   * @throws ClassCastException if the class is not a {@link KijiPivoter}.
   */
  public static Class<? extends KijiPivoter> forName(String className)
      throws ClassNotFoundException {
    return Class.forName(className).asSubclass(KijiPivoter.class);
  }
}
