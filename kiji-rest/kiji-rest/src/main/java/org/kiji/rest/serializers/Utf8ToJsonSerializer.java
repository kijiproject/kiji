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
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.apache.avro.util.Utf8;

/**
 * Avro's specific types don't seem to serialize properly so this class provides
 * the necessary hook to convert an Avro SpecificRecordBase to a Json object that
 * can properly be serialized and sent back to the client.
 *
 */
public class Utf8ToJsonSerializer extends JsonSerializer<Utf8> {

  /**
   * {@inheritDoc}
   */
  @Override
  public void serialize(Utf8 record, JsonGenerator generator, SerializerProvider provider)
      throws IOException {
    generator.writeString(record.toString());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<Utf8> handledType() {
    return Utf8.class;
  }
}
