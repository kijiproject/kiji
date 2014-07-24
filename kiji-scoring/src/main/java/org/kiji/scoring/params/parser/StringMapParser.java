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

package org.kiji.scoring.params.parser;

import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Maps;

import org.kiji.scoring.params.IllegalParamValueException;
import org.kiji.scoring.params.ParamSpec;
import org.kiji.scoring.params.ValueParser;

/**
 * Parser for a map of strings to strings. Key-value pairs are separated by "|", and keys are
 * separated from values by "=".
 * For example: "key1=value1|key2=value2|key3=value3".
 * If a key is associated with multiple values, the last one is used.
 */
@SuppressWarnings("rawtypes")
public class StringMapParser implements ValueParser<Map> {

  public static final String PAIR_SEPARATOR = "\\|";
  public static final String KEY_VALUE_SEPARATOR = "=";


  /** {@inheritDoc} */
  @Override
  public Class<? extends Map> getParsedClass() {
    return Map.class;
  }

  /** {@inheritDoc} */
  @Override
  public boolean parsesSubclasses() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> parse(ParamSpec param, String value) {
    final TreeMap<String, String> map = Maps.newTreeMap();
    String[] values = value.split(PAIR_SEPARATOR);

    for (String keyValue : values) {
      if (keyValue == null) {
        throw new IllegalParamValueException(param, keyValue);
      }
      final String[] split = keyValue.split(KEY_VALUE_SEPARATOR, 2);
      if (split.length != 2) {
        throw new IllegalParamValueException(param, keyValue);
      }
      map.put(split[0], split[1]);
    }
    return map;
  }
}
