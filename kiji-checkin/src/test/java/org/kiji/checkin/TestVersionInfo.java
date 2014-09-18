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

package org.kiji.checkin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import org.kiji.delegation.NamedProvider;

/**
 * Tests the functionality of {@link VersionInfo}. These tests are dependent on the system
 * property <code>kiji-bento-test-version</code> being set by the testing infrastructure.
 * Currently this property is set in the Maven configuration for the Surefire plugin.
 */
public class TestVersionInfo {

  @Test
  public void testGetSoftwareVersion() throws IOException {
    assertEquals("Software version does not match Maven version.",
        "unknown",
        VersionInfo.getSoftwareVersion(this.getClass()));
  }

  @Test
  public void testShouldGetProperSoftwarePackage() throws IOException {
    assertEquals("Kiji Dynamic Binding Library", VersionInfo.
        getSoftwarePackage(NamedProvider.class));
  }
}
