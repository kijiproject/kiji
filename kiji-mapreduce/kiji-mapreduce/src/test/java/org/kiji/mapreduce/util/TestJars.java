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
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that the Jars utility class works okay.  We make sure that it throws a
 * ClassNotFoundException if it can't find the class in a jar, but we can't do better than
 * that from within a unit test.
 */
public class TestJars {
  /** Configuration variable name to store jars names in. */
  private static final String CONF_TMPJARS = "tmpjars";

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  /** A temporary directory to store jars in. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  @Test
  public void testGetJarPathForClassNotFound() throws ClassNotFoundException, IOException {
    try {
      // This will throw an exception because this class is not in a jar when being run.
      Jars.getJarPathForClass(TestJars.class);
      fail("ClassNotFoundException");
    } catch (ClassNotFoundException ce) {
      assertEquals("Unable to find containing jar for class org.kiji.mapreduce.util.TestJars",
      ce.getMessage());
    }
  }
}
