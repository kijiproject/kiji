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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class TestByteUtils {

  @Test
  public void testByteTransformations() {
    final String string = "frobnicate";

    Assert.assertEquals(string, ByteUtils.toString(ByteUtils.toBytes(string)));

    Assert.assertEquals(
        string,
        ByteUtils.toString(
            ByteUtils.toBytes(
                ByteBuffer.wrap(
                    ByteUtils.toBytes(string)))));
  }

  @Test
  public void testToStringBinary() {
    final byte[] bytes = new byte[] { 0x63, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x00 };

    Assert.assertEquals("cstring\\x00", ByteUtils.toStringBinary(bytes));
    Assert.assertEquals("string", ByteUtils.toStringBinary(bytes, 1, bytes.length - 2));
  }
}
