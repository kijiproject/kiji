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

package org.kiji.hive.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * Convenience classes for serializing/deserializing Writable objects into byte[]s.
 */
public final class ByteWritable {
  /**
   * Utility class cannot be instantiated.
   */
  private ByteWritable() {
  }

  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = null;
    try {
      dataOut = new DataOutputStream(out);
      writable.write(dataOut);
      return out.toByteArray();
    } finally {
      IOUtils.closeQuietly(dataOut);
    }
  }

  public static <T extends Writable> T asWritable(byte[] bytes, Class<T> clazz)
      throws IOException {
    T result = null;
    DataInputStream dataIn = null;
    try {
      result = (T) WritableFactories.newInstance(clazz);
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      dataIn = new DataInputStream(in);
      result.readFields(dataIn);
    } finally {
      IOUtils.closeQuietly(dataIn);
    }
    return result;
  }
}
