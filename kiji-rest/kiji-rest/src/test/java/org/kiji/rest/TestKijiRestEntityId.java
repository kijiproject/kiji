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

package org.kiji.rest;

import static org.junit.Assert.*;


import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONObject;

import org.junit.Test;

import org.kiji.rest.representations.KijiRestEntityId;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.tools.ToolUtils;
import org.kiji.schema.util.Hasher;

public class TestKijiRestEntityId {
  // Unusual UTF-16 string with Cyrillic, Imperial Phoenician, etc.
  private static final String UNUSUAL_STRING_EID = "abcde\b\fa0€ÿҖ𐤇";
  private static final String SINGLE_COMPONENT_EID =
      String.format("[%s]", JSONObject.quote(UNUSUAL_STRING_EID));

  @Test
  public void testShouldWorkWithRKFRawLayout() throws Exception {
    final TableLayoutDesc desc =
        KijiTableLayouts.getLayout("org/kiji/rest/layouts/rkf_raw.json");
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);
    final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
    final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);
    final KijiRestEntityId restEid1 = KijiRestEntityId.create(originalEid, layout);
    final KijiRestEntityId restEid2 = KijiRestEntityId.create(
        String.format("hbase=%s", Bytes.toStringBinary(rowKey)));
    final KijiRestEntityId restEid3 = KijiRestEntityId.create(
        String.format("hbase_hex=%s", new String(Hex.encodeHex((rowKey)))));

    // Resolved entity id should match origin entity id.
    assertEquals(originalEid, restEid1.resolve(layout));
    assertEquals(originalEid, restEid2.resolve(layout));
    assertEquals(originalEid, restEid3.resolve(layout));
  }

  @Test
  public void testShouldWorkWithRKFHashedLayout() throws Exception {
    final TableLayoutDesc desc =
        KijiTableLayouts.getLayout("org/kiji/rest/layouts/rkf_hashed.json");
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

    // Byte array representations of row keys should work.
    final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
    final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);
    final KijiRestEntityId restEid1 = KijiRestEntityId.create(originalEid, layout);
    final KijiRestEntityId restEid2 = KijiRestEntityId.create(
        String.format("hbase=%s", Bytes.toStringBinary(rowKey)));
    final KijiRestEntityId restEid3 = KijiRestEntityId.create(
        String.format("hbase_hex=%s", new String(Hex.encodeHex((rowKey)))));

    // Resolved entity id should match origin entity id.
    assertEquals(originalEid, restEid1.resolve(layout));
    assertEquals(originalEid, restEid2.resolve(layout));
    assertEquals(originalEid, restEid3.resolve(layout));

    // Component representation of entities should work.
    final KijiRestEntityId restEid4 = KijiRestEntityId.create(SINGLE_COMPONENT_EID);
    final EntityId resolvedEid = restEid4.resolve(layout);
    final String recoveredComponent = Bytes.toString(
        Bytes.toBytesBinary(
            resolvedEid.toShellString().substring(
                ToolUtils.KIJI_ROW_KEY_SPEC_PREFIX.length())));
    assertEquals(UNUSUAL_STRING_EID, recoveredComponent);
    assertEquals(resolvedEid, restEid4.resolve(layout));
  }

  @Test
  public void testShouldWorkWithRKFHashPrefixedLayout() throws Exception {
    final TableLayoutDesc desc =
        KijiTableLayouts.getLayout("org/kiji/rest/layouts/rkf_hashprefixed.json");
    final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

    // Byte array representations of row keys should work.
    // Prepend appropriate hashed prefix to UNUSUAL_STRING_EID.
    final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
    final byte[] hash = Hasher.hash(Bytes.toBytes(UNUSUAL_STRING_EID));
    final byte[] hbaseRowKey = new byte[rowKey.length + 2];
    System.arraycopy(hash, 0, hbaseRowKey, 0, 2);
    System.arraycopy(rowKey, 0, hbaseRowKey, 2, rowKey.length);
    final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(hbaseRowKey);
    final KijiRestEntityId restEid1 = KijiRestEntityId.create(originalEid, layout);
    final KijiRestEntityId restEid2 = KijiRestEntityId.create(
        String.format("hbase=%s", Bytes.toStringBinary(hbaseRowKey)));
    final KijiRestEntityId restEid3 = KijiRestEntityId.create(
        String.format("hbase_hex=%s", new String(Hex.encodeHex((hbaseRowKey)))));

    // Resolved entity id should match origin entity id.
    assertEquals(originalEid, restEid1.resolve(layout));
    assertEquals(originalEid, restEid2.resolve(layout));
    assertEquals(originalEid, restEid3.resolve(layout));

    // Component representation of entities should work.
    final KijiRestEntityId restEid4 = KijiRestEntityId.create(SINGLE_COMPONENT_EID);
    final EntityId resolvedEid = restEid4.resolve(layout);
    final String recoveredComponent = Bytes.toString(
        Bytes.toBytesBinary(
            resolvedEid.toShellString().substring(
                ToolUtils.KIJI_ROW_KEY_SPEC_PREFIX.length())));
    assertEquals(UNUSUAL_STRING_EID, recoveredComponent);
    assertEquals(resolvedEid, restEid4.resolve(layout));
  }

    @Test
    public void testShouldWorkWithRKF2RawLayout() throws Exception {
      final TableLayoutDesc desc =
          KijiTableLayouts.getLayout("org/kiji/rest/layouts/rkf2_raw.json");
      final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
      final EntityIdFactory factory = EntityIdFactory.getFactory(layout);
      final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
      final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);
      final KijiRestEntityId restEid1 = KijiRestEntityId.create(originalEid, layout);
      final KijiRestEntityId restEid2 = KijiRestEntityId.create(
          String.format("hbase=%s", Bytes.toStringBinary(rowKey)));
      final KijiRestEntityId restEid3 = KijiRestEntityId.create(
          String.format("hbase_hex=%s", new String(Hex.encodeHex((rowKey)))));

      // Resolved entity id should match origin entity id.
      assertEquals(originalEid, restEid1.resolve(layout));
      assertEquals(originalEid, restEid2.resolve(layout));
      assertEquals(originalEid, restEid3.resolve(layout));
    }

    @Test
    public void testShouldWorkWithRKF2SuppressedLayout() throws Exception {
      final TableLayoutDesc desc =
          KijiTableLayouts.getLayout("org/kiji/rest/layouts/rkf2_suppressed.json");
      final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
      final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

      // Construct complex entity id.
      final String eidString = String.format("[%s,%s,%s,%d,%d]",
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          Integer.MIN_VALUE,
          Long.MAX_VALUE);
      final EntityId originalEid = ToolUtils.createEntityIdFromUserInputs(eidString, layout);
      final KijiRestEntityId restEid1 = KijiRestEntityId.create(originalEid, layout);
      final KijiRestEntityId restEid2 = KijiRestEntityId.create(eidString);
      final KijiRestEntityId restEid3 = KijiRestEntityId.create(
          String.format("hbase_hex=%s",
              new String(Hex.encodeHex((originalEid.getHBaseRowKey())))));
      final KijiRestEntityId restEid4 = KijiRestEntityId.create(
              String.format("hbase=%s", Bytes.toStringBinary(originalEid.getHBaseRowKey())));

      // Resolved entity id should match origin entity id.
      assertEquals(originalEid, restEid1.resolve(layout));
      assertEquals(originalEid, restEid2.resolve(layout));
      assertEquals(originalEid, restEid3.resolve(layout));
      assertEquals(originalEid, restEid4.resolve(layout));
    }

    @Test
    public void testShouldWorkWithRKF2FormattedLayout() throws Exception {
      final TableLayoutDesc desc =
          KijiTableLayouts.getLayout("org/kiji/rest/layouts/rkf2_suppressed.json");
      final KijiTableLayout layout = KijiTableLayout.newLayout(desc);
      final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

      // Construct complex entity id.
      final String eidString = String.format("[%s,%s,%s,%d,%d]",
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          Integer.MIN_VALUE,
          Long.MAX_VALUE);
      final EntityId originalEid = ToolUtils.createEntityIdFromUserInputs(eidString, layout);
      final KijiRestEntityId restEid1 = KijiRestEntityId.create(originalEid, layout);
      final KijiRestEntityId restEid2 = KijiRestEntityId.create(eidString);
      final KijiRestEntityId restEid3 = KijiRestEntityId.create(
          String.format("hbase_hex=%s",
              new String(Hex.encodeHex((originalEid.getHBaseRowKey())))));
      final KijiRestEntityId restEid4 = KijiRestEntityId.create(
              String.format("hbase=%s", Bytes.toStringBinary(originalEid.getHBaseRowKey())));
      final EntityId toolUtilsEid = ToolUtils.createEntityIdFromUserInputs(eidString, layout);

      // Resolved entity id should match origin entity id.
      assertEquals(originalEid, restEid1.resolve(layout));
      assertEquals(originalEid, restEid2.resolve(layout));
      assertEquals(originalEid, restEid3.resolve(layout));
      assertEquals(originalEid, restEid4.resolve(layout));
      assertEquals(toolUtilsEid, restEid4.resolve(layout));
    }
}
