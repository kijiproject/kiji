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

package org.kiji.scoring.params;

import java.util.Map;

import com.google.common.collect.Maps;

import org.kiji.schema.KijiColumnName;
import org.kiji.scoring.FreshenerGetStoresContext;


/**
 * Testing context for parameter parsing.
 */

public class TestContext implements FreshenerGetStoresContext {
  private Map<String, String> mMap = Maps.newHashMap();

  /**
   * Empty constructor.
   */
  public TestContext() {
  }

  /**
   * Simple constructor - the arguement is split by "=", and this is used as
   * the key and value for the map.
   * @param arg The argument to be split.
   */
  public TestContext(String arg) {
    String[] keyValue = arg.split("=");

    // Successfully split the argument
    if (keyValue.length >= 2) {
      mMap.put(keyValue[0], keyValue[1]);
      return;
    }
    if (arg.contains("=")) {
      mMap.put(keyValue[0], "");
      return;
    } else {
      mMap.put(arg, null);
    }
  }

  /**
   * Simple constructor with the key and value specified.
   * @param key The key.
   * @param value The value.
   */
  public TestContext(String key, String value) {
    mMap.put(key, value);
  }

  /** {@inheritDoc} */
  @Override
  public String getParameter(String key) {
    return mMap.get(key);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> getParameters() {
    return mMap;
  }

  /** {@inheritDoc} */
  @Override
  public KijiColumnName getAttachedColumn() {
    return null;
  }
}
