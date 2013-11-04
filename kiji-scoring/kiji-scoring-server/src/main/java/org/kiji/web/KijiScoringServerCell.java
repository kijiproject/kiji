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

package org.kiji.web;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.schema.impl.AvroCellEncoder;

/**
 * Models what a Kiji cell looks like when returned to the client. If the value set in the cell
 * is an Avro record, then the value will be serialized into a JSON string (so the client can
 * deserialize this back to an Avro object).
 */
@JsonPropertyOrder({ "timestamp", "value" })
public class KijiScoringServerCell {

  @JsonProperty("family")
  private String mFamily;

  @JsonProperty("qualifier")
  private String mQualifier;

  @JsonProperty("timestamp")
  private Long mTimestamp;

  @JsonProperty("value")
  private String mValue;

  @JsonProperty("schema")
  private String mSchema;

  /**
   * Constructs a KijiScoringServerCell given a timestamp and value.
   *
   * @param family is the family of the cell.
   * @param qualifier is the qualifier of the cell.
   * @param timestamp is the timestamp of the cell.
   * @param value is the cell's value. If this is an Avro object, then it will be serialized
   *        to JSON and returned as a string literal containing JSON data.
   * @throws IOException if there is a problem serializing the value into JSON.
   */
  public KijiScoringServerCell(
      final String family,
      final String qualifier,
      final Long timestamp,
      final Object value
  ) throws IOException {
    mTimestamp = timestamp;
    mValue = getJsonString(value);
    mSchema = getSchema(value).toString();
    mFamily = family;
    mQualifier = qualifier;
  }

  /** Default constructor for Jackson. **/
  public KijiScoringServerCell() { }

  /**
   * Get the cell's timestamp.
   *
   * @return the cell's timestamp
   */
  public Long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Get the cell's datum value.
   *
   * @return the cell's datum value
   */
  public Object getValue() {
    return mValue;
  }

  /**
   * Returns an encoded JSON string for the given object.
   *
   * @param input datum to encode as a JSON string.
   * @return the JSON string representing this object.
   *
   * @throws IOException if there is an error encoding the datum.
   */
  private static String getJsonString(
      final Object input
  ) throws IOException {

    final String jsonString;
    if (input instanceof GenericContainer) {
      final GenericContainer record = (GenericContainer) input;
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), baos);
        if (record instanceof SpecificRecord) {
          new SpecificDatumWriter<GenericContainer>(record.getSchema()).write(record, encoder);
        } else {
          new GenericDatumWriter<GenericContainer>(record.getSchema()).write(record, encoder);
        }
        encoder.flush();
        jsonString = new String(baos.toByteArray(), Charset.forName("UTF-8"));
      } finally {
        baos.close();
      }
    } else {
      jsonString = input.toString();
    }

    return jsonString;
  }

  /**
   * Returns the Avro Schema for this value.
   *
   * @param value is the record whose schema to return
   * @return the Schema object of the input value.
   */
  private static Schema getSchema(
      final Object value
  ) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema();
    }
    final Schema primitiveSchema = AvroCellEncoder.PRIMITIVE_SCHEMAS
        .get(value.getClass().getCanonicalName());
    if (null != primitiveSchema) {
      return primitiveSchema;
    }
    throw new RuntimeException(String.format("Unsupported output type found."
        + "Class: %s", value.getClass().getCanonicalName()));
  }
}
