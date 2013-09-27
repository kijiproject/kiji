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

package org.kiji.rest.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.kiji.rest.representations.SchemaOption;

/**
 * Converts a JSON object to a SchemaOption.
 *
 */
public class JsonToSchemaOption extends JsonDeserializer<SchemaOption> {

  /**
   * {@inheritDoc}
   */
  @Override
  public SchemaOption deserialize(JsonParser parser, DeserializationContext context)
      throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    JsonNode schemaNode = mapper.readTree(parser);
    SchemaOption returnSchema = null;
    if (schemaNode.isInt()) {
      returnSchema = new SchemaOption(schemaNode.asLong());
    } else {
      returnSchema = new SchemaOption(schemaNode.textValue());
    }

    return returnSchema;
  }

}
