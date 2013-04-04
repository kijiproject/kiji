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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

import org.kiji.schema.KijiDataRequest;

/**
 * Serializes and deserializes a KijiDataRequest such that it is safe for Job configuration.
 */
public final class KijiDataRequestSerializer {

  /** Utility class cannot be instantiated. */
  private KijiDataRequestSerializer() {}

  /**
   * Serializes a data request.
   *
   * @param dataRequest A data request.
   * @return The serialized data request.
   * @throws IOException If there is an error.
   */
  public static String serialize(KijiDataRequest dataRequest) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    try {
      oos.writeObject(dataRequest);
      oos.flush();
      return Base64.encodeBase64String(baos.toByteArray());
    } finally {
      IOUtils.closeQuietly(oos);
    }
  }

  /**
   * Deserializes a data request.
   *
   * @param serialized A serialized data request.
   * @return A data request, or null if there is no data request in the given string.
   * @throws IOException If there is an IO error.
   */
  public static KijiDataRequest deserialize(String serialized) throws IOException {
    final byte[] bytes = Base64.decodeBase64(serialized);
    final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
    try {
      return (KijiDataRequest) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(ois);
    }
  }
}

