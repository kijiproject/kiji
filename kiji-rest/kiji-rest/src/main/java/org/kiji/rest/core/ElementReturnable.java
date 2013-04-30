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

import java.util.Collection;

/**
 * Returnable object with a name and null contents.
 */
public class ElementReturnable implements Returnable {
  private final String mName;
  private long mCounter;

  /**
   * Constructor with a String-typed name of the object.
   * @param name The name of the object.
   */
  public ElementReturnable(String name) {
    mName = name;
    mCounter = 0;
  }

  /**
   * Constructor with a String-typed name of the object and the initial value of the counter.
   * @param name The name of the object
   * @param counter The initial value of the counter.
   */
  public ElementReturnable(String name, long counter) {
    mName = name;
    mCounter = counter;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return mName;
  }

  /** {@inheritDoc} */
  @Override
  public Collection<Returnable> getContents() {
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public void setCounter(long counter) {
    mCounter = counter;
  }

  /** {@inheritDoc} */
  @Override
  public long getCounter() {
    return mCounter;
  }

  /**
   * Increment the value of the counter.
   */
  public void incrementCounter() {
    mCounter++;
  }
}
