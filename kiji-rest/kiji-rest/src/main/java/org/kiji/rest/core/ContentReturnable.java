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

import java.util.ArrayList;
import java.util.List;

public class ContentReturnable extends ElementReturnable {
  private List<Returnable> mContents;

  /**
   * Constructor with a String-typed name of the object.
   * @param name The name of the object.
   */
  public ContentReturnable(String name) {
    super(name);
    mContents = new ArrayList<Returnable>();
  }

  /**
   * Constructor with a String-typed name of the object.
   * @param name The name of the object.
   */
  public ContentReturnable(String name, long counter) {
    super(name, counter);
  }

  /**
   * Add child content to this object.
   * @param child The Returnable content to be added.
   */
  public void add(Returnable child) {
    mContents.add(child);
  }

  /** {@inheritDoc} */
  @Override
  public List<Returnable> getContents() {
    return mContents;
  }
}
