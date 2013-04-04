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

package org.kiji.hive.utils;

import static org.kiji.hive.utils.HiveTypes.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * Converts an Avro data object to an in-memory representation for Hive.
 *
 * <p>This prepares hive objects under the assumption that we are
 * using the "standard" object inspectors. See the classes in
 * org.apache.hadoop.hive.serde2.objectinspector that start with
 * "Standard" for details about how each Hive type should be formatted.</p>
 */
public enum AvroTypeAdapter {
  /** Singleton instance. */
  INSTANCE;

  /**
   * Indicates that an Avro data type is not compatible with a Hive type.
   */
  public static class IncompatibleTypeException extends RuntimeException {
    /**
     * Constructor.
     *
     * @param hiveType The hive type.
     * @param avroData The avro data.
     */
    public IncompatibleTypeException(TypeInfo hiveType, Object avroData) {
      super("Unable to convert avro data [" + avroData + "] to hive type [" + hiveType + "]");
    }

    /**
     * Constructor.
     *
     * @param hiveType The hive type.
     */
    public IncompatibleTypeException(TypeInfo hiveType) {
      super("Unable to generate an avro schema that describes hive type [" + hiveType + "]");
    }
  }

  /**
   * Generates a generic avro reader schema from a hive type declaration.
   *
   * @param hiveType The hive type to generate an avro schema for.
   * @return An avro reader schema suitable for reading data necessary
   *     to fill the hive type.
   */
  public Schema toAvroSchema(TypeInfo hiveType) {
    // Special case the Avro union with null, which in effect just means the type is "nullable".
    // In Hive, all types are nullable, so {T, null} and {null, T} should become just T in hive.

    return Schema.createUnion(
        Arrays.asList(Schema.create(Schema.Type.NULL), toNonNullableAvroSchema(hiveType)));
  }

  /**
   * Generates a generic avro reader schema from a hive type
   * declaration that is not nullable.
   *
   * @param hiveType The hive type to generate an avro schema for.
   * @return An avro reader schema suitable for reading data necessary
   *     to fill the hive type.
   */
  public Schema toNonNullableAvroSchema(TypeInfo hiveType) {
    switch (hiveType.getCategory()) {
    case PRIMITIVE:
      return toNonNullableAvroSchema((PrimitiveTypeInfo) hiveType);
    case LIST:
      final ListTypeInfo listTypeInfo = (ListTypeInfo) hiveType;
      return Schema.createArray(toAvroSchema(listTypeInfo.getListElementTypeInfo()));
    case MAP:
      final MapTypeInfo mapTypeInfo = (MapTypeInfo) hiveType;
      // TODO: Validate that the map key type is a string.
      return Schema.createMap(toAvroSchema(mapTypeInfo.getMapValueTypeInfo()));
    case STRUCT:
      final StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType;
      final List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
      final List<TypeInfo> fieldTypes = structTypeInfo.getAllStructFieldTypeInfos();
      final List<Schema.Field> fields = new ArrayList<Schema.Field>();
      for (int i = 0; i < fieldNames.size(); i++) {
        final String fieldName = fieldNames.get(i);
        final TypeInfo fieldType = fieldTypes.get(i);
        // TODO: Figure out whether we need to specify an empty doc
        //       string, or if null will work just fine.
        fields.add(new Schema.Field(fieldName, toAvroSchema(fieldType), "", null));
      }
      return Schema.createRecord(fields);
    case UNION:
      // TODO: Figure out what we should do with Hive union types.
      throw new UnsupportedOperationException();
    default:
      throw new IncompatibleTypeException(hiveType);
    }
  }

  /**
   * Constructs a non-nullable Avro schema from a primitive Hive type.
   *
   * @param primitiveType The Hive type.
   * @return An avro schema.
   */
  public Schema toNonNullableAvroSchema(PrimitiveTypeInfo primitiveType) {
    switch (primitiveType.getPrimitiveCategory()) {
    case VOID: // Like the avro null type, right?
      return Schema.create(Schema.Type.NULL);
    case BYTE:
      return Schema.createFixed("BYTE", "", "", 1);
    case SHORT:
      return Schema.createFixed("SHORT", "", "", 2);
    case BOOLEAN:
      return Schema.create(Schema.Type.BOOLEAN);
    case INT:
      return Schema.create(Schema.Type.INT);
    case LONG:
      return Schema.create(Schema.Type.LONG);
    case FLOAT:
      return Schema.create(Schema.Type.FLOAT);
    case DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case STRING:
      return Schema.create(Schema.Type.STRING);
    case TIMESTAMP:
      return Schema.create(Schema.Type.LONG);
    case BINARY:
      return Schema.create(Schema.Type.BYTES);
    default:
      throw new IncompatibleTypeException(primitiveType);
    }
  }

  /**
   * Converts a piece avro data to a hive in-memory object.
   *
   * @param hiveType The type of the target hive object.
   * @param avro The avro data to convert.
   * @return The converted hive datum, compatible with the standard object inspector.
   */
  public Object toHiveType(TypeInfo hiveType, Object avro) {
    if (null == avro) {
      return null;
    }

    switch (hiveType.getCategory()) {
    case PRIMITIVE:
      return toHiveType((PrimitiveTypeInfo) hiveType, avro);
    case LIST:
      HiveList<Object> hiveList = new HiveList<Object>();
      @SuppressWarnings("unchecked")
      final List<Object> avroList = (List<Object>) avro;
      for (Object avroElement : avroList) {
        hiveList.add(toHiveType(((ListTypeInfo) hiveType).getListElementTypeInfo(), avroElement));
      }
      return hiveList;
    case MAP:
      HiveMap<String, Object> hiveMap = new HiveMap<String, Object>();
      @SuppressWarnings("unchecked")
      final Map<CharSequence, Object> avroMap = (Map<CharSequence, Object>) avro;
      for (Map.Entry<CharSequence, Object> avroEntry : avroMap.entrySet()) {
        final TypeInfo mapValueType = ((MapTypeInfo) hiveType).getMapValueTypeInfo();
        hiveMap.put(avroEntry.getKey().toString(), toHiveType(mapValueType, avroEntry.getValue()));
      }
      return hiveMap;
    case STRUCT:
      HiveStruct hiveStruct = new HiveStruct();
      final GenericRecord avroRecord = (GenericRecord) avro;
      final StructTypeInfo hiveStructType = (StructTypeInfo) hiveType;
      for (int i = 0; i < hiveStructType.getAllStructFieldNames().size(); i++) {
        final String fieldName = hiveStructType.getAllStructFieldNames().get(i);
        final TypeInfo fieldType = hiveStructType.getAllStructFieldTypeInfos().get(i);
        hiveStruct.add(toHiveType(fieldType, avroRecord.get(fieldName)));
      }
      return hiveStruct;
    case UNION:
      throw new UnsupportedOperationException("Union types are not supported.");
    default:
      throw new IncompatibleTypeException(hiveType, avro);
    }
  }

  /**
   * Converts data from Avro into a Hive primitive type.
   *
   * @param primitiveType The target Hive type.
   * @param avro The avro datum.
   * @return The converted Hive object.
   */
  public Object toHiveType(PrimitiveTypeInfo primitiveType, Object avro) {
    switch (primitiveType.getPrimitiveCategory()) {

    case VOID: // Like the avro null type, right?
      return null;

    case BYTE:
      if (!(avro instanceof GenericFixed)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      return Byte.valueOf(((GenericFixed) avro).bytes()[0]);

    case SHORT:
      if (!(avro instanceof GenericFixed)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      final byte[] fixedBytes = ((GenericFixed) avro).bytes();
      final ByteBuffer bb = ByteBuffer.allocate(2);
      bb.order(ByteOrder.LITTLE_ENDIAN);
      bb.put(fixedBytes[0]);
      bb.put(fixedBytes[1]);
      return bb.getShort(0);

    case BOOLEAN: // These primitive types are the same in avro and hive.
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
      return avro;

    case STRING:
      return avro.toString();

    case TIMESTAMP:
      if (!(avro instanceof Long)) {
        throw new IncompatibleTypeException(primitiveType, avro);
      }
      return new Timestamp(((Long) avro).longValue());

    case BINARY:
      final ByteArrayRef byteArrayRef = new ByteArrayRef();
      final ByteBuffer byteBuffer = (ByteBuffer) avro;
      byteArrayRef.setData(
          Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit()));
      return byteArrayRef;

    default:
      throw new IncompatibleTypeException(primitiveType, avro);
    }
  }
}
