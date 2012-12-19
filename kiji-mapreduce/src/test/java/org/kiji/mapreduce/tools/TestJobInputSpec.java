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

package org.kiji.mapreduce.tools;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestJobInputSpec {
  @Test
  public void testNormalConstructor() {
    JobInputSpec spec = new JobInputSpec(JobInputSpec.Format.TEXT, "/tmp/foo");
    assertEquals(JobInputSpec.Format.TEXT, spec.getFormat());
    assertEquals("/tmp/foo", spec.getLocation());
  }

  @Test
  public void testParseText() throws JobIOSpecParseException {
    JobInputSpec spec = JobInputSpec.parse("text:hdfs://localhost:8020/tmp/foo");
    assertEquals(JobInputSpec.Format.TEXT, spec.getFormat());
    assertEquals("hdfs://localhost:8020/tmp/foo", spec.getLocation());
  }

  @Test
  public void testParseMultipleInputs() throws JobIOSpecParseException {
    JobInputSpec spec = JobInputSpec.parse("text:hdfs:/tmp/foo,hdfs:/tmp/bar");
    assertEquals(JobInputSpec.Format.TEXT, spec.getFormat());
    assertEquals(2, spec.getLocations().length);
    assertEquals("hdfs:/tmp/foo", spec.getLocations()[0]);
    assertEquals("hdfs:/tmp/bar", spec.getLocations()[1]);
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testOneKijiTableAsInput() throws JobIOSpecParseException {
    JobInputSpec.parse("kiji:foo,bar"); // Only one table may be specified as input.
  }

  @Test(expected=JobIOSpecParseException.class)
  public void testParseInvalidFormat() throws JobIOSpecParseException {
    JobInputSpec.parse("invalid:foo");
  }

  @Test(expected=JobIOSpecParseException.class)
  public void testParseNoPath() throws JobIOSpecParseException {
    JobInputSpec.parse("invalid");
  }
}
