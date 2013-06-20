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

package org.kiji.delegation;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestAutoLookup {
  public static class Widget {

    @AutoLookup
    private ConfiguredIfaceA mIfaceA;

    @AutoLookup
    private ConfiguredIfaceB mIfaceB;

    // Because this is non-null, shouldn't be overwritten by Delegation.init().
    @AutoLookup
    private ConfiguredIfaceB mSecondB = new ConfiguredImplB2();

    // Should be left as null, since we don't annotate it.
    private ConfiguredIfaceB mIgnored;

    public Widget() {
      Delegation.init(this);
    }

    public String getStringVal() {
      return mIfaceA.getMessage();
    }

    public int getIntVal() {
      return mIfaceB.getVal();
    }

    public int getSecondIntVal() {
      return mSecondB.getVal(); // should be '2'
    }

    public boolean isIgnoredNull() {
      return null == mIgnored;
    }
  }

  @Test
  public void testAutoLookup() {
    // Create a test widget which auto-initializes through lazy binding in its c'tor.
    // These are configured by src/test/resources/kiji-delegation.properties. Interfaces
    // not specified there are configured through a BasicLookup that looks in META-INF/services/.
    Widget widget = new Widget();

    // Assert that the Widget autoconfigured itself with the correct implementations
    // of the interface-typed fields.
    assertEquals("Hello", widget.getStringVal());
    assertEquals(1, widget.getIntVal());
    assertEquals(2, widget.getSecondIntVal());
    assertTrue(widget.isIgnoredNull());
  }
}

