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

package org.kiji.common.flags;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.kiji.common.flags.parser.SimpleValueParser;

/** Tests for custom flag parsers. */
public class TestCustomParser {

  public static final String SENTINEL = "jackpot";

  public static class CustomParser extends SimpleValueParser<String> {
    /** {@inheritDoc} */
    @Override
    public Class<? extends String> getParsedClass() {
      return String.class;
    }

    /** {@inheritDoc} */
    @Override
    public String parse(FlagSpec flag, String value) {
      return SENTINEL;
    }
  }

  private static final class TestObject {
    @Flag(parser = CustomParser.class)
    public String custom = null;
  }

  @Test
  public void testCustomFlagParser() {
    final TestObject test = new TestObject();
    FlagParser.init(test, new String[]{"--custom=ignored"});
    assertEquals(SENTINEL, test.custom);
  }
}
