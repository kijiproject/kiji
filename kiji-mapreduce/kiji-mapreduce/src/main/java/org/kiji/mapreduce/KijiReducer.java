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

package org.kiji.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;

/**
 * Base class for Kiji reducer.
 *
 * @param <INKEY> The type of the input key to the mapper.
 * @param <INVALUE> The type of the input value to the mapper.
 * @param <OUTKEY> The type of the output key from the mapper.
 * @param <OUTVALUE> The type of the output value from the mapper.
 */
@ApiAudience.Public
@Inheritance.Extensible
public abstract class KijiReducer<INKEY, INVALUE, OUTKEY, OUTVALUE>
    extends Reducer<INKEY, INVALUE, OUTKEY, OUTVALUE>
    implements KVOutputJob {

  /**
   * Loads a KijiReducer class by name.
   *
   * @param className Fully qualified name of the class to load.
   * @return the loaded class.
   * @throws ClassNotFoundException if the class is not found.
   * @throws ClassCastException if the class is not a KijiReducer.
   */
  @SuppressWarnings("rawtypes")
  public static Class<? extends KijiReducer> forName(String className)
      throws ClassNotFoundException {
    return Class.forName(className).asSubclass(KijiReducer.class);
  }
}
