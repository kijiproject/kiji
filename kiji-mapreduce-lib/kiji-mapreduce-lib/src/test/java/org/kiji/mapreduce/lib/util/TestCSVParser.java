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

package org.kiji.mapreduce.lib.util;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestCSVParser {

  private void assertStringListsEqual(List<String> expecteds, List<String> actuals) {
    Assert.assertEquals("Number of parsed fields", expecteds.size(), actuals.size());
    Iterator<String> expectedItr = expecteds.iterator();
    Iterator<String> actualItr = actuals.iterator();
    while (expectedItr.hasNext()) {
      String expected = expectedItr.next();
      String actual = actualItr.next();
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testDeriveFields() throws ParseException {
    String line = "first,last,phone";
    List<String> derivedFields = CSVParser.parseCSV(line);
    List<String> expected = Arrays.asList("first", "last", "phone");
    assertStringListsEqual(expected, derivedFields);

  }

  @Test(expected = ParseException.class)
  public void testNonleadingQuote()  throws ParseException {
    String line = " \"first\"";
    CSVParser.parseCSV(line);
  }

  @Test(expected = ParseException.class)
  public void testUnmatchedEscapeCharacters()  throws ParseException {
    String line = "\"first";
    CSVParser.parseCSV(line);
  }

  @Test(expected = ParseException.class)
  public void testEndingWithAnEscapedDoubleQuote()  throws ParseException {
    String line = "\"first\"\"";
    CSVParser.parseCSV(line);
  }

  @Test
  public void testDoubleQuoteFollowedByComma()  throws ParseException {
    String line = "\"first\"\",\"";
    List<String> derivedFields = CSVParser.parseCSV(line);
    List<String> expected = Arrays.asList("first\",");
    assertStringListsEqual(expected, derivedFields);
  }

  @Test
  public void testEscaping() throws ParseException {
    String line = "first,\"last\",phone";
    List<String> derivedFields = CSVParser.parseCSV(line);

    List<String> expected = Arrays.asList("first", "last", "phone");
    assertStringListsEqual(expected, derivedFields);
  }

  @Test
  public void testDoubleQuoteInsideField() throws ParseException {
    String line = "first,\"la\"\"st\",phone";
    List<String> derivedFields = CSVParser.parseCSV(line);
    List<String> expected = Arrays.asList("first", "la\"st", "phone");
    assertStringListsEqual(expected, derivedFields);
  }

  @Test
  public void testCommasInsideOfFields() throws ParseException {
    String line = "John Doe,\"San Francisco, CA\",94110";
    List<String> derivedFields = CSVParser.parseCSV(line);
    List<String> expected = Arrays.asList("John Doe", "San Francisco, CA", "94110");
    assertStringListsEqual(expected, derivedFields);
  }

  @Test
  public void testTabSeparatedValuess() throws ParseException {
    String line = "John Doe\tSan Francisco, CA\t94110";
    List<String> derivedFields = CSVParser.parseTSV(line);
    List<String> expected = Arrays.asList("John Doe", "San Francisco, CA", "94110");
    assertStringListsEqual(expected, derivedFields);
  }
}
