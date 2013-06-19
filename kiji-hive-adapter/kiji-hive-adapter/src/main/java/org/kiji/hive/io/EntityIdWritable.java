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

package org.kiji.hive.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdException;

/**
 * Container class for the data stored within an entityId.
 */
public class EntityIdWritable implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(EntityIdWritable.class);

  private byte[] mHBaseRowKey;
  private List<Object> mComponents;
  private String mShellString;

  /**
   * Enumeration for the possible types of the component.
   */
  private static enum Component {
    STRING,
    INTEGER,
    LONG,
    NULL,
    RAW_HBASE_KEY
  }

  /** Required so that this can be built by WritableFactories. */
  public EntityIdWritable() {}

  /**
   * Constructs an EntityIdWritable from an existing EntityId.
   *
   * @param entityId from a KijiRowData.
   */
  public EntityIdWritable(EntityId entityId) {
    mHBaseRowKey = entityId.getHBaseRowKey();
    mComponents = entityId.getComponents();
    mShellString = entityId.toShellString();
  }

  /** @return the HBase row key. */
  public byte[] getHBaseRowKey() {
    return Arrays.copyOf(mHBaseRowKey, mHBaseRowKey.length);
  }

  /** @return List of Objects representing the individual components of a row key. */
  public List<Object> getComponents() {
    return Collections.unmodifiableList(mComponents);
  }

  /** @return A copyable string. */
  public String toShellString() {
    return mShellString;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeCompressedByteArray(out, mHBaseRowKey);

    // Write the components
    WritableUtils.writeVInt(out, mComponents.size());
    for (Object component : mComponents) {
      if (component instanceof String) {
        WritableUtils.writeEnum(out, Component.STRING);
        String stringComponent = (String) component;
        WritableUtils.writeString(out, stringComponent);
      } else if (component instanceof Integer) {
        WritableUtils.writeEnum(out, Component.INTEGER);
        Integer intComponent = (Integer) component;
        WritableUtils.writeVInt(out, intComponent);
      } else if (component instanceof Long) {
        WritableUtils.writeEnum(out, Component.LONG);
        Long longComponent = (Long) component;
        WritableUtils.writeVLong(out, longComponent);
      } else if (component instanceof byte[]) {
        Preconditions.checkState(mComponents.size() == 1, "byte[] only valid as sole component.");
        WritableUtils.writeEnum(out, Component.RAW_HBASE_KEY);
        byte[] byteArrayComponent = (byte[]) component;
        WritableUtils.writeCompressedByteArray(out, byteArrayComponent);
      } else if (component == null) {
        WritableUtils.writeEnum(out, Component.NULL);
      } else {
        throw new EntityIdException("Unexpected type for Component "
            + component.getClass().getName());
      }
    }

    WritableUtils.writeString(out, mShellString);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] bytes = WritableUtils.readCompressedByteArray(in);
    mHBaseRowKey = bytes;

    // Read the components
    int numComponents = WritableUtils.readVInt(in);
    List<Object> components = Lists.newArrayList();
    for (int c=0; c < numComponents; c++) {
      Component componentType = WritableUtils.readEnum(in, Component.class);
      switch(componentType) {
        case STRING:
          String stringComponent = WritableUtils.readString(in);
          components.add(stringComponent);
          break;
        case INTEGER:
          Integer intComponent = WritableUtils.readVInt(in);
          components.add(intComponent);
          break;
        case LONG:
          Long longComponent = WritableUtils.readVLong(in);
          components.add(longComponent);
          break;
        case RAW_HBASE_KEY:
          byte[] byteArrayComponent = WritableUtils.readCompressedByteArray(in);
          components.add(byteArrayComponent);
          break;
        case NULL:
          break;
        default:
          throw new EntityIdException("Unexpected type for Component " + componentType);
      }
    }
    mComponents = components;

    String shellString = WritableUtils.readString(in);
    mShellString = shellString;
  }
}
