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

import java.util.List;

import com.google.common.collect.Lists;

import org.kiji.scoring.params.ParamSpec;
import org.kiji.scoring.params.ValueParser;

/**
 * Parser for a list of strings, separated by "|".
 * For example: "value1|value2|value3".
 */
@SuppressWarnings("rawtypes")
public class StringListParser implements ValueParser<List> {

  public static final String SEPARATOR = "\\|";

  /** {@inheritDoc} */
  @Override
  public Class<? extends List> getParsedClass() {
    return List.class;
  }

  /** {@inheritDoc} */
  @Override
  public boolean parsesSubclasses() {
    return true;
  }


  /** {@inheritDoc} */
  @Override
  public List<String> parse(ParamSpec param, String values) {
    String[] splitValues = values.split(SEPARATOR);
    return Lists.newArrayList(splitValues);
  }
}
