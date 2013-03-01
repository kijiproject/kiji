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

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.parser.StringParser;

/**
 * Tests the behavior of parsers for an entire set of subclasses.
 */
public class TestBaseParsers {
  private static final Logger LOG = LoggerFactory.getLogger(TestBaseParsers.class);

  public static class CharSequenceParser implements ValueParser<CharSequence> {
    /** {@inheritDoc} */
    @Override
    public Class<? extends CharSequence> getParsedClass() {
      return CharSequence.class;
    }

    /** {@inheritDoc} */
    @Override
    public boolean parsesSubclasses() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public CharSequence parse(FlagSpec flag, List<String> values) {
      return values.get(values.size() - 1);
    }
  }

  public static class StringSubClassParser implements ValueParser<String> {
    /** {@inheritDoc} */
    @Override
    public Class<? extends String> getParsedClass() {
      return String.class;
    }

    /** {@inheritDoc} */
    @Override
    public boolean parsesSubclasses() {
      return true;
    }

    /** {@inheritDoc} */
    @Override
    public String parse(FlagSpec flag, List<String> values) {
      return values.get(values.size() - 1);
    }
  }

  private static final class TestObject {
    @Flag(parser = CharSequenceParser.class)
    public String custom = null;
  }

  @Test
  public void testBaseParser() {
    final TestObject test = new TestObject();
    FlagParser.init(test, new String[]{"--custom=string_value"});
    assertEquals("string_value", test.custom);
  }

  @Test
  public void testParserMapSubClassConflict() {
    final Map<Class<?>, ValueParser<?>> parserMap = Maps.newHashMap();
    final Map<Class<?>, ValueParser<?>> baseParserMap = Maps.newHashMap();
    final List<ValueParser> parsers =
        Lists.<ValueParser>newArrayList(new CharSequenceParser(), new StringSubClassParser());
    try {
      FlagSpec.buildParserMap(parsers, parserMap, baseParserMap);
    } catch (RuntimeException re) {
      LOG.debug("Expected exception: {}", re.getMessage());
      assertEquals(
          "Conflicting command-line argument parsers for subclasses of 'java.lang.String' : "
          + "'org.kiji.common.flags.TestBaseParsers$CharSequenceParser' "
          + "and 'org.kiji.common.flags.TestBaseParsers$StringSubClassParser'.",
          re.getMessage());
    }
  }

  @Test
  public void testParserMapExactConflict() {
    final Map<Class<?>, ValueParser<?>> parserMap = Maps.newHashMap();
    final Map<Class<?>, ValueParser<?>> baseParserMap = Maps.newHashMap();
    final List<ValueParser> parsers =
        Lists.<ValueParser>newArrayList(new StringParser(), new StringParser());
    try {
      FlagSpec.buildParserMap(parsers, parserMap, baseParserMap);
    } catch (RuntimeException re) {
      LOG.debug("Expected exception: {}", re.getMessage());
      assertEquals(
          "Conflicting command-line argument parsers for type 'java.lang.String' : "
          + "'org.kiji.common.flags.parser.StringParser' "
          + "and 'org.kiji.common.flags.parser.StringParser'.",
          re.getMessage());
    }
  }
}
