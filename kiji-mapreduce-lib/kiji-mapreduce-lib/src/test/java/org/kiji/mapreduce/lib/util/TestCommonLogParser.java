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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.kiji.mapreduce.lib.util.CommonLogParser.Field;

import java.text.ParseException;
import java.util.Map;

import org.junit.Test;

public class TestCommonLogParser {
  @Test
  public void testDeriveFields() throws ParseException {
    // CSOFF: LineLengthCheck
    String line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200 2326";
    Map<Field, String> derivedFields = CommonLogParser.get().parseCommonLog(line);
    assertEquals("127.0.0.1", derivedFields.get(Field.REMOTEHOST));
    assertEquals("user-identifier", derivedFields.get(Field.IDENT));
    assertEquals("frank", derivedFields.get(Field.AUTHUSER));
    assertEquals("10/Oct/2000:13:55:36 -0700", derivedFields.get(Field.DATE));
    assertEquals("GET /apache_pb.gif HTTP/1.0", derivedFields.get(Field.REQUEST));
    assertEquals("200", derivedFields.get(Field.STATUS));
    assertEquals("2326", derivedFields.get(Field.BYTES));
  }

  @Test
  public void testParserExceptions() {
    String line = "";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Could not parse empty line.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse ident.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse authuser.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse date.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank garbage";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Date field needs to begin with a [", pe.getMessage());
      assertEquals(33, pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse date.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700]";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse request.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] garbage";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Request field needs to begin with a \"", pe.getMessage());
      assertEquals(62, pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] \"GET";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse request.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\"";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse status.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }

    line = "127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] \"GET /apache_pb.gif HTTP/1.0\" 200";
    try {
      CommonLogParser.get().parseCommonLog(line);
      fail("Should've gotten a ParseException.");
    } catch (ParseException pe) {
      assertEquals("Not enough tokens to parse bytes.", pe.getMessage());
      assertEquals(line.length(), pe.getErrorOffset());
    }
  }
}
