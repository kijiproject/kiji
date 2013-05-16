/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class TestLists {
  @Test
  public void testDistinct() {
    List<String> inputs = new ArrayList<String>();
    inputs.add("a");
    inputs.add("b");
    inputs.add("a");
    inputs.add("a");

    List<String> outputs = new ArrayList<String>();
    outputs.add("a");
    outputs.add("b");

    assertTrue(outputs.equals(Lists.distinct(inputs)));
  }

  @Test
  public void testDistinct2() {
    // Demonstrate input order preservation.
    List<String> inputs = new ArrayList<String>();
    inputs.add("b");
    inputs.add("a");
    inputs.add("a");
    inputs.add("a");

    List<String> outputs = new ArrayList<String>();
    outputs.add("b");
    outputs.add("a");

    assertTrue(outputs.equals(Lists.distinct(inputs)));
  }

  @Test
  public void testDistinct3() {
    List<String> inputs = new ArrayList<String>();
    inputs.add("a");
    inputs.add("b");
    inputs.add("c");

    assertTrue(inputs.equals(Lists.distinct(inputs)));

    inputs.clear();
    assertTrue(inputs.equals(Lists.distinct(inputs)));
  }

  @Test
  public void testDistinctNulls() {
    List<String> inputs = new ArrayList<String>();
    inputs.add("b");
    inputs.add(null);
    inputs.add(null);
    inputs.add("a");

    List<String> outputs = new ArrayList<String>();
    outputs.add("b");
    outputs.add(null);
    outputs.add("a");

    assertTrue(outputs.equals(Lists.distinct(inputs)));
  }

  @Test
  public void testMap() {
    List<Integer> inputs = new ArrayList<Integer>();
    inputs.add(Integer.valueOf(3));
    inputs.add(Integer.valueOf(42));

    List<Integer> expected = new ArrayList<Integer>();
    expected.add(Integer.valueOf(6));
    expected.add(Integer.valueOf(84));

    // Use a 'times 2' mapper.
    List<Integer> actuals = Lists.map(inputs, new Lists.Func<Integer, Integer>() {
      @Override
      public Integer eval(Integer in) {
        return Integer.valueOf(2 * in.intValue());
      }
    });

    assertTrue(expected.equals(actuals));
  }

  @Test
  public void testFoldLeft() {
    List<String> inputs = new ArrayList<String>();
    inputs.add("a");
    inputs.add("b");
    inputs.add("c");

    String stringified = Lists.foldLeft(new StringBuilder(), inputs,
        new Lists.Aggregator<String, StringBuilder>() {
          @Override
          public StringBuilder eval(String in, StringBuilder out) {
            if (0 != out.length()) {
              out.append(",");
            }

            out.append(in);
            return out;
          }
        }).toString();
    assertEquals("a,b,c", stringified);
  }

  @Test
  public void testFoldRight() {
    List<String> inputs = new ArrayList<String>();
    inputs.add("a");
    inputs.add("b");
    inputs.add("c");

    String stringified = Lists.foldRight(new StringBuilder(), inputs,
        new Lists.Aggregator<String, StringBuilder>() {
          @Override
          public StringBuilder eval(String in, StringBuilder out) {
            if (0 != out.length()) {
              out.append(",");
            }

            out.append(in);
            return out;
          }
        }).toString();
    assertEquals("c,b,a", stringified);
  }

  @Test
  public void testToArray() {
    String[] oneString = Lists.toArray(Collections.singletonList("foo"), String.class);
    assertEquals(1, oneString.length);
    assertEquals("foo", oneString[0]);

    oneString = Lists.toArray(Collections.<String>singletonList(null), String.class);
    assertEquals(1, oneString.length);
    assertNull(oneString[0]);

    String[] emptyOut = Lists.toArray(Collections.<String>emptyList(), String.class);
    assertNotNull(emptyOut);
    assertEquals(0, emptyOut.length);

    String[] nulOut = Lists.<String>toArray(null, String.class);
    assertNotNull(nulOut);
    assertEquals(0, nulOut.length);
  }
}
