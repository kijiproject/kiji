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

package org.kiji.mapreduce.bulkimport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import org.kiji.mapreduce.bulkimport.BinaryHTableBulkImporter.BinaryHBaseCellDecoder;
import org.kiji.mapreduce.bulkimport.BinaryHTableBulkImporter.ColumnDescriptor;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiColumnName;

public class TestBinaryHTableBulkImporter {
  @Test
  public void testBinaryHBaseCellDecoder() throws IOException {
    BinaryHBaseCellDecoder decoder = new BinaryHBaseCellDecoder();

    DecodedCell<?> booleanCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.BOOLEAN, Bytes.toBytes(true));
    assertEquals(Schema.create(Schema.Type.BOOLEAN), booleanCell.getWriterSchema());
    assertTrue((Boolean) booleanCell.getData());

    DecodedCell<?> doubleCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.DOUBLE, Bytes.toBytes(3.2));
    assertEquals(Schema.create(Schema.Type.DOUBLE), doubleCell.getWriterSchema());
    assertEquals(3.2, (Double) doubleCell.getData(), 1e-10);

    DecodedCell<?> floatCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.FLOAT, Bytes.toBytes(3.1f));
    assertEquals(Schema.create(Schema.Type.FLOAT), floatCell.getWriterSchema());
    assertEquals(3.1f, (Float) floatCell.getData(), 1e-10);

    DecodedCell<?> bytesCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.BYTES, new byte[] {1, 2, 3});
    assertEquals(Schema.create(Schema.Type.BYTES), bytesCell.getWriterSchema());
    assertArrayEquals(new byte[] {1, 2, 3}, ((ByteBuffer) bytesCell.getData()).array());

    DecodedCell<?> intCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.INT, Bytes.toBytes(3));
    assertEquals(Schema.create(Schema.Type.INT), intCell.getWriterSchema());
    assertEquals(3, ((Integer) intCell.getData()).intValue());

    DecodedCell<?> longCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.LONG, Bytes.toBytes(42L));
    assertEquals(Schema.create(Schema.Type.LONG), longCell.getWriterSchema());
    assertEquals(42L, ((Long) longCell.getData()).longValue());

    DecodedCell<?> shortCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.SHORT, Bytes.toBytes((short) 4));
    assertEquals(Schema.create(Schema.Type.INT), shortCell.getWriterSchema());
    assertEquals(4, ((Integer) shortCell.getData()).intValue());

    DecodedCell<?> stringCell = decoder.decode(
        BinaryHBaseCellDecoder.Type.STRING, Bytes.toBytes("hi"));
    assertEquals(Schema.create(Schema.Type.STRING), stringCell.getWriterSchema());
    assertEquals("hi", ((CharSequence) stringCell.getData()).toString());
  }

  @Test
  public void testColumnDescriptor() throws IOException {
    ColumnDescriptor columnDescriptor = ColumnDescriptor.parse("foo:bar:type:info:name");
    assertEquals("foo", Bytes.toString(columnDescriptor.getHBaseFamilyBytes()));
    assertEquals("bar", Bytes.toString(columnDescriptor.getHBaseQualifierBytes()));
    assertEquals("type", columnDescriptor.getType());
    assertEquals(new KijiColumnName("info:name"), columnDescriptor.getKijiColumnName());
  }

  @Test
  public void testGetOutputColumn() throws IOException {
    Configuration conf = new Configuration();
    conf.set(BinaryHTableBulkImporter.CONF_COLUMN_DESCRIPTORS,
        "foo:bar:int:info:id,foo:baz:string:info:name");

    BinaryHTableBulkImporter importer = new BinaryHTableBulkImporter();
    importer.setConf(conf);
  }

  @Test
  public void testScannerNoBlockCaching() throws IOException {
    Configuration conf = new Configuration();
    conf.set(BinaryHTableBulkImporter.CONF_COLUMN_DESCRIPTORS,
        "foo:bar:int:info:id,foo:baz:string:info:name");
    BinaryHTableBulkImporter importer = new BinaryHTableBulkImporter();
    importer.setConf(conf);
    Scan scan = importer.getInputHTableScan(conf);
    assertFalse(scan.getCacheBlocks());
  }
}
