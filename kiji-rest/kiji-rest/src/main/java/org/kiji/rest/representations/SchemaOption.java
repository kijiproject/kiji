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

package org.kiji.rest.representations;

import java.io.IOException;

import org.apache.avro.Schema;

import org.kiji.schema.KijiSchemaTable;

/**
 * Class that holds two (mutually exclusive) representations of an Avro schema. This can either
 * hold the JSON string representation of the schema (that gets parsed by the Schema.Parser class)
 * or the unique identifier that Kiji assigns to new Avro schemas.
 *
 */
public class SchemaOption {

  private static final long UNDEFINED_SCHEMA_ID = -1L;
  private Schema mParsedSchema = null;
  private long mSchemaUid = UNDEFINED_SCHEMA_ID;
  private Schema.Parser mParser = new Schema.Parser();

  /**
   * Construct a new SchemaOption specifying the schema as the JSON string.
   *
   * @param schemaString is the JSON string representation of the Avro Schema.
   */
  public SchemaOption(String schemaString) {
    mParsedSchema = mParser.parse(schemaString);
  }

  /**
   * Construct a new SchemaOption specifying the schema UID.
   *
   * @param schemaUid is the Kiji assigned UID for the schema.
   */
  public SchemaOption(long schemaUid) {
    mSchemaUid = schemaUid;
  }

  /**
   * Produce an Avro schema object depending on what was set (string or UID).
   *
   * @param schemaTable is the schema table to use to fetch the Avro schema given a UID
   * @return the corresponding Avro schema.
   * @throws IOException if there is a problem fetching the schema from the schema table if the
   *         UID is set.
   */
  public Schema resolve(KijiSchemaTable schemaTable) throws IOException {
    if (mParsedSchema == null) {
      return schemaTable.getSchema(mSchemaUid);
    } else {
      return mParsedSchema;
    }
  }

  /**
   * Returns either the UID or the schema string representation. If the UID is -1 then it will
   * try and return the JSON representation of the Avro Schema. If neither work, then returns
   * null. The UID is instance specific and is only used to avoid resultset bloating since the JSON
   * representation of an Avro schema could be large and sending that for each cell can be large.
   *
   * @return whatever representation of the schema is set (long UID, schema string or null if
   *         the UID doesn't resolve to a proper schema object).
   */
  public Object getOptionValue() {
    if (mSchemaUid >= 0) {
      return new Long(mSchemaUid);
    } else if (mParsedSchema != null) {
      return mParsedSchema.toString();
    } else {
      return null;
    }
  }
}
