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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Models what a Kiji cell looks like when returned to the client. The value property can
 * store one of three things (two data types but three unique meanings):
 * <ol>
 *   <li> Numbers (ints, longs, floats, doubles etc)
 *   <li> String literals
 *   <li> String literals that are JSON objects represented as escaped strings.
 * </ol>
 * The JSON string within a value is used when the underlying cell value is a more complex
 * Avro type (arrays, maps, unions, records). For more information about the JSON encoding
 * of Avro records, please consult the Avro
 * <a href="http://avro.apache.org/docs/current/spec.html#json_encoding">spec</a>. When setting
 * a cell value to a complex Avro type, please ensure that the string is properly escaped so that
 * it's not interpreted as a JSON object but rather as a JSON string.
 */
@JsonPropertyOrder({"timestamp", "value"})
public class KijiRestCell {

  @JsonProperty("timestamp")
  private Long mTimestamp;

  @JsonProperty("value")
  private Object mValue;

  /**
   * Constructs a KijiRestCell given a timestamp and value.
   *
   * @param timestamp is the timestamp of the cell.
   * @param value is the cell's value.
   */
  public KijiRestCell(Long timestamp, Object value) {
    mTimestamp = timestamp;
    mValue = value;
  }

  /**
   * Dummy constructor required for Jackson to (de)serialize JSON properly.
   */
  public KijiRestCell() {
  }

  /**
   * Returns the underlying cell's timestamp.
   *
   * @return the underlying cell's timestamp
   */
  public Long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Returns the underlying cell's column value.
   *
   * @return the underlying cell's column value
   */
  public Object getValue() {
    return mValue;
  }
}
