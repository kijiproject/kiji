/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  WibiData, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.kiji.common.flags.parser;

import java.util.List;

import org.kiji.common.flags.FlagSpec;
import org.kiji.common.flags.ValueParser;

/**
 * Parser for a list of strings.
 *
 * Usage example:
 *   <code>tool --flag=value1 --flag=value2 --value=value3</code>
 */
public class StringListParser implements ValueParser<List> {
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
  public List<String> parse(FlagSpec flag, List<String> values) {
    return values;
  }
}