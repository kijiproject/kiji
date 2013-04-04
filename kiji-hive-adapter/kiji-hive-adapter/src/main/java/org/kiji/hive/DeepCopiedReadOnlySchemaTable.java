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

package org.kiji.hive;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;

import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.avro.SchemaTableBackup;
import org.kiji.schema.avro.SchemaTableEntry;
import org.kiji.schema.util.BytesKey;

/**
 * A SchemaTable implementation that is run off of a backup of a KijiSchemaTable.
 *
 * <p>
 *   This class exists because we need to have a schema table available during the Hive
 *   deserialization process. However, we can't leave a HTable connection open because there
 *   is no hook to close it after a query executes. Therefore, we open it during initialization,
 *   perform a deep copy so we can use it during deserialization, and close the connection.
 * </p>
 *
 */
public class DeepCopiedReadOnlySchemaTable implements KijiSchemaTable {
  private final SchemaTableBackup mSchemaTableBackup;

  /** Backup of all of the schema table entries by id. */
  private final Map<Long, SchemaTableEntry> mIdToSchemaTableEntryMap;

  /** Lazily loaded id to schema map. */
  private final Map<Long, Schema> mIdToSchemaMap;

  /**
   * Constructs a schema table, copying the given one.
   *
   * @param toCopy The schema table to copy.
   * @throws IOException if there was an issue copying the schema table.
   */
  public DeepCopiedReadOnlySchemaTable(KijiSchemaTable toCopy) throws IOException {
    mSchemaTableBackup = toCopy.toBackup();
    mIdToSchemaTableEntryMap = Maps.newHashMap();
    for (SchemaTableEntry schemaTableEntry : mSchemaTableBackup.getEntries()) {
      mIdToSchemaTableEntryMap.put(schemaTableEntry.getId(), schemaTableEntry);
    }
    mIdToSchemaMap = Maps.newHashMap();
  }

  @Override
  /**
   * {@inheritDoc}
   *
   * Backed by the internal representation derived from a SchemaTableBackup.
   */
  public Schema getSchema(long schemaId) throws IOException {
    if (!mIdToSchemaMap.containsKey(schemaId)) {
      // Cache miss, search and decode the id.
      final SchemaTableEntry schemaTableEntry = mIdToSchemaTableEntryMap.get(schemaId);
      final String schemaJson = schemaTableEntry.getAvroSchema();
      final Schema schema = new Schema.Parser().parse(schemaJson);
      mIdToSchemaMap.put(schemaId, schema);
    }
    return mIdToSchemaMap.get(schemaId);
  }

  @Override
  public long getOrCreateSchemaId(Schema schema) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public BytesKey getOrCreateSchemaHash(Schema schema) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public BytesKey getSchemaHash(Schema schema) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Schema getSchema(BytesKey schemaHash) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaEntry getSchemaEntry(long schemaId) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaEntry getSchemaEntry(BytesKey schemaHash) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaEntry getSchemaEntry(Schema schema) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaTableBackup toBackup() throws IOException {
    return mSchemaTableBackup;
  }

  @Override
  public void fromBackup(SchemaTableBackup backup) throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void flush() throws IOException {
    throw new UnsupportedOperationException("Not implemented");
  }
}
