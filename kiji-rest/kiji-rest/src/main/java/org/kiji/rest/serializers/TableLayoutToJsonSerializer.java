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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.util.ToJson;

/**
 * Ensures that the TableLayoutDesc avro class can be written to the client as a proper
 * JSON string.
 *
 */
public class TableLayoutToJsonSerializer extends JsonSerializer<TableLayoutDesc> {

  private ObjectMapper mJsonMapper = new ObjectMapper();

  @Override
  public void serialize(TableLayoutDesc layout, JsonGenerator generator,
      SerializerProvider provider)
      throws IOException {
    String jsonString = ToJson.toAvroJsonString(layout);
    JsonNode node = mJsonMapper.readTree(jsonString);
    generator.writeTree(node);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<TableLayoutDesc> handledType() {
    return TableLayoutDesc.class;
  }

}
