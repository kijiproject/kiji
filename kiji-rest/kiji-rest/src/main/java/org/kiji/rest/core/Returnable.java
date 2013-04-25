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

package org.kiji.rest.core;

import java.util.List;

/**
 * Interface for key-value objects with String-typed name of the object.
 * Contents are a collection of returnable objects.
 * Replace with Avro key-value object going-forward.
 */
public interface Returnable {
  /**
   * Get the String key of the Returnable object.
   * @return key The String-typed name.
   */
  public String getName();

  /**
   * Get the Collection of Returnable objects as contents.
   * @return The Collection-typed contents.
   */
  public List<? extends Returnable> getContents();

  /**
   * Get the value of the counter.
   * @return The counter.
   */
  public long getCounter();

  /**
   * Set the value of the counter.
   * @param counter
   */
  public void setCounter(long counter);
}