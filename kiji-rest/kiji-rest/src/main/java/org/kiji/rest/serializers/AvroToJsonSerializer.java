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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

/**
 * Avro's specific types don't seem to serialize properly so this class provides
 * the necessary hook to convert an Avro SpecificRecordBase to a Json object that
 * can properly be serialized and sent back to the client.
 *
 */
public class AvroToJsonSerializer extends JsonSerializer<GenericContainer> {

  private ObjectMapper mJsonObjectMapper = new ObjectMapper();

  //CSOFF: JavadocMethodCheck
  /**
   * @see JsonSerializer#serialize(Object, JsonGenerator, SerializerProvider)
   */
  @Override
  public void serialize(GenericContainer record, JsonGenerator generator,
      SerializerProvider provider) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), os);
    GenericDatumWriter<GenericContainer> writer = new GenericDatumWriter<GenericContainer>();
    writer.setSchema(record.getSchema());
    writer.write(record, encoder);
    encoder.flush();
    String jsonString = new String(os.toByteArray());
    os.close();
    JsonNode node = mJsonObjectMapper.readTree(jsonString);
    generator.writeTree(node);
  }

  /**
   * @see JsonSerializer#handledType()
   */
  @Override
  public Class<GenericContainer> handledType() {
    // TODO Auto-generated method stub
    return GenericContainer.class;
  }
  //CSON: JavadocMethodCheck
}
