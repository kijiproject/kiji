/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.commons;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests of the resource tracker.
 */
public class TestResourceTracker {

  @Test
  public void testTrackedProxy() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final Closeable proxy = ResourceTracker.getTrackedProxy(Closeable.class, baos);
    Assert.assertEquals(baos.hashCode(), proxy.hashCode());
    Assert.assertEquals(baos.toString(), proxy.toString());
    Assert.assertTrue(ResourceTracker.get().isResourceRegistered(baos));
    proxy.close();
    Assert.assertFalse(ResourceTracker.get().isResourceRegistered(baos));
  }

  @Test
  public void testUnregisterUntrackedResource() throws Exception {
    // Unregistering an untracked resource should not throw, but it should log verbosely.
    ResourceTracker.get().unregisterResource(this);
  }
}
