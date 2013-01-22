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

public class TestJobOutputSpec {
  @Test
  public void testNullPathConstructor() {
    JobOutputSpec spec = JobOutputSpec.create(JobOutputSpec.Format.KIJI, null, 1);
    assertEquals(JobOutputSpec.Format.KIJI, spec.getFormat());
    assertEquals(null, spec.getLocation());
    assertEquals(1, spec.getSplits());
  }

  @Test
  public void testNormalConstructor() {
    JobOutputSpec spec = JobOutputSpec.create(JobOutputSpec.Format.TEXT, "/tmp/foo", 2);
    assertEquals(JobOutputSpec.Format.TEXT, spec.getFormat());
    assertEquals("/tmp/foo", spec.getLocation());
    assertEquals(2, spec.getSplits());
  }

  @Test
  public void testParseKiji() throws JobIOSpecParseException {
    JobOutputSpec spec = JobOutputSpec.parse("kiji");
    assertEquals(JobOutputSpec.Format.KIJI, spec.getFormat());
    assertEquals(null, spec.getLocation());
  }

  @Test
  public void testParseKijiColon() throws JobIOSpecParseException {
    JobOutputSpec spec = JobOutputSpec.parse("kiji:");
    assertEquals(JobOutputSpec.Format.KIJI, spec.getFormat());
    assertEquals(null, spec.getLocation());
  }

  @Test
  public void testParseText() throws JobIOSpecParseException {
    JobOutputSpec spec = JobOutputSpec.parse("text:/tmp/foo@1");
    assertEquals(JobOutputSpec.Format.TEXT, spec.getFormat());
    assertEquals("/tmp/foo", spec.getLocation());
    assertEquals(1, spec.getSplits());
  }

  @Test
  public void testParseHFile() throws JobIOSpecParseException {
    JobOutputSpec spec = JobOutputSpec.parse("hfile:/tmp/foo@8");
    assertEquals(JobOutputSpec.Format.HFILE, spec.getFormat());
    assertEquals("/tmp/foo", spec.getLocation());
    assertEquals(8, spec.getSplits());
  }

  @Test
  public void testParseMapWithColon() throws JobIOSpecParseException {
    JobOutputSpec spec = JobOutputSpec.parse("map:hdfs://localhost:8000/tmp/foo@2");
    assertEquals(JobOutputSpec.Format.MAP_FILE, spec.getFormat());
    assertEquals("hdfs://localhost:8000/tmp/foo", spec.getLocation());
    assertEquals(2, spec.getSplits());
  }

  @Test(expected=JobIOSpecParseException.class)
  public void testInvalidFormat() throws JobIOSpecParseException {
    JobOutputSpec.parse("invalid:hdfs://localhost:8000/tmp/foo");
  }

  @Test(expected=JobIOSpecParseException.class)
  public void testMissingRequiredPath() throws JobIOSpecParseException {
    JobOutputSpec.parse("avro");
  }

  @Test(expected=JobIOSpecParseException.class)
  public void testMissingRequiredPathWithColon() throws JobIOSpecParseException {
    JobOutputSpec.parse("seq:");
  }

  @Test(expected=JobIOSpecParseException.class)
  public void testMissingSplits() throws JobIOSpecParseException {
    JobOutputSpec.parse("seq:asdf");
  }
}
