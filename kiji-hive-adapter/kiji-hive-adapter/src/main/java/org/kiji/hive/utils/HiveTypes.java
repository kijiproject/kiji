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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;

/**
 * Definitions of how we represent hive types in Java.
 */
public final class HiveTypes {

  /** Utility class cannot be instantiated. */
  private HiveTypes() {}

  /** A Hive STRUCT. */
  public static class HiveStruct extends ArrayList<Object> {
    private static final long serialVersionUID = 1L;
  }

  /** A Hive UNION. */
  public static class HiveUnion implements UnionObject {
    private byte mTag;
    private Object mObject;

    /**
     * Sets the tag for the object inside of the union.
     * @param tag that defines what the type inside of the union is.
     */
    public void setTag(Byte tag) {
      mTag = tag;
    }

    /**
     * Sets the object inside of this union.
     * @param hiveObject that is of the type specified by the tag.
     */
    public void setObject(Object hiveObject) {
      mObject = hiveObject;
    }

    @Override
    /** {@inheritDoc} */
    public byte getTag() {
      return mTag;
    }

    @Override
    /** {@inheritDoc} */
    public Object getObject() {
      return mObject;
    }
  }

  /** A Hive ARRAY. */
  public static class HiveList<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;
  }

  /** A Hive MAP. */
  public static class HiveMap<K, V> extends HashMap<K, V> {
    private static final long serialVersionUID = 1L;
  }
}
