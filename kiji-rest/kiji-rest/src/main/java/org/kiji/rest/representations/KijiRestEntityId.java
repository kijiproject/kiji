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
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import com.google.common.collect.Lists;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.avro.ComponentType;
import org.kiji.schema.avro.HashSpec;
import org.kiji.schema.avro.RowKeyEncoding;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Container class for entity ids which can be backed as strings
 * (suitable for raw, hashed, hash-prefixed, and materialization suppressed keys)
 * xor as a list of components
 * (suitable for formatted entity ids without materialization unsuppresed).
 */
public final class KijiRestEntityId {
  private static final ObjectMapper BASIC_MAPPER = new ObjectMapper();

  /**
   * Prefixes for specifying hex row keys.
   */
  public static final String HBASE_ROW_KEY_PREFIX = "hbase=";
  public static final String HBASE_HEX_ROW_KEY_PREFIX = "hbase_hex=";

  /**
   * Back eid as either string or list of components.
   */
  private final String mStringEntityId;
  private final Object[] mComponents;

  /**
   * This field is only relevant w.r.t. wildcarded lists of components.
   */
  private final boolean mIsWildcarded;

  /**
   * Private constructor for KijiRestEntityId parametrized by a String.
   * Validate fields as necessary.
   *
   * @param stringEntityId string representing the entity id.
   */
  private KijiRestEntityId(
      final String stringEntityId) {
    // stringEntityIdId may not be null.
    Preconditions.checkNotNull(stringEntityId);
    // StringEntityId must be prefixed by "hbase=" or "hbase_hex=".
    Preconditions.checkArgument(stringEntityId.startsWith(HBASE_ROW_KEY_PREFIX)
        || stringEntityId.startsWith(HBASE_HEX_ROW_KEY_PREFIX));
    mStringEntityId = stringEntityId;
    mComponents = null;
    mIsWildcarded = false;
  }

  /**
   * Private constructor for KijiRestEntityId parametrized by an array of components.
   * Validate fields as necessary.
   *
   * @param components of the formatted entity id.
   * @param wildCarded if one of the components is a wildcard.
   */
  private KijiRestEntityId(
      final Object[] components,
      final boolean wildCarded) {
    // Only one of stringEntityId or components array may be specified.
    Preconditions.checkNotNull(components);
    // Wildcarded flag is only applicable for components array.
    Preconditions.checkArgument(components.length > 0);
    mStringEntityId = null;
    mComponents = components;
    mIsWildcarded = wildCarded;
  }

  /**
   * Create KijiRestEntityId from a string input, which can be a raw hbase rowKey prefixed by
   * 'hbase=' or 'hbase_hex=' (the former for bytes-encoding and the latter for ASCII encoding).
   *
   * @param entityId string of the row.
   * @return a properly constructed KijiRestEntityId.
   * @throws IOException if KijiRestEntityId can not be properly constructed.
   */
  public static KijiRestEntityId create(
      final String entityId) throws IOException {
    return new KijiRestEntityId(entityId);
  }

  /**
   * Create KijiRestEntityId from a string input, which can be a json string or a raw hbase rowKey.
   * This method is used for entity ids specified from the URL.
   *
   * @param entityId string of the row.
   * @param layout of the table in which the entity id belongs.
   *        If null, then long components may not be recognized.
   * @return a properly constructed KijiRestEntityId.
   * @throws IOException if KijiRestEntityId can not be properly constructed.
   */
  public static KijiRestEntityId createFromUrl(
      final String entityId,
      final KijiTableLayout layout) throws IOException {
    if (entityId.startsWith(HBASE_ROW_KEY_PREFIX)
        || entityId.startsWith(HBASE_HEX_ROW_KEY_PREFIX)) {
      return new KijiRestEntityId(entityId);
    } else {
      final JsonParser parser = new JsonFactory().createJsonParser(entityId)
          .enable(Feature.ALLOW_COMMENTS)
          .enable(Feature.ALLOW_SINGLE_QUOTES)
          .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
      final JsonNode node = BASIC_MAPPER.readTree(parser);
      return create(node, layout);
    }
  }

  /**
   * Create a list of KijiRestEntityIds from a string input, which can be a json array of valid
   * entity id strings and/or valid hbase row keys.
   * This method is used for entity ids specified from the URL.
   *
   * @param entityIdListString string of a json array of rows identifiers.
   * @param layout of the table in which the entity id belongs.
   *        If null, then long components may not be recognized.
   * @return a properly constructed list of KijiRestEntityIds.
   * @throws IOException if KijiRestEntityId list can not be properly constructed.
   */
  public static List<KijiRestEntityId> createListFromUrl(
      final String entityIdListString,
      final KijiTableLayout layout) throws IOException {
    final JsonParser parser = new JsonFactory().createJsonParser(entityIdListString)
        .enable(Feature.ALLOW_COMMENTS)
        .enable(Feature.ALLOW_SINGLE_QUOTES)
        .enable(Feature.ALLOW_UNQUOTED_FIELD_NAMES);
    final JsonNode jsonNode = BASIC_MAPPER.readTree(parser);

    List<KijiRestEntityId> kijiRestEntityIds = Lists.newArrayList();

    if (jsonNode.isArray()) {
      for (JsonNode node : jsonNode) {
        if (node.isTextual()) {
          kijiRestEntityIds.add(createFromUrl(node.textValue(), layout));
        } else {
          kijiRestEntityIds.add(createFromUrl(node.toString(), layout));
        }
      }
    } else {
      throw new IOException("The entity id list string is not a valid json array.");
    }

    return kijiRestEntityIds;
  }

  /**
   * Create KijiRestEntityId from entity id and layout.
   *
   * @param entityId of the row.
   * @param layout of the table containing the row.
   * @return a properly constructed KijiRestEntityId.
   * @throws IOException if KijiRestEntityId can not be properly constructed.
   */
  public static KijiRestEntityId create(
      final EntityId entityId,
      final KijiTableLayout layout) throws IOException {
    final Object keysFormat = layout.getDesc().getKeysFormat();
    final RowKeyEncoding encoding = getEncoding(keysFormat);
    switch (encoding) {
    case HASH_PREFIX:
      return new KijiRestEntityId(
          new Object[] { Bytes.toString((byte[]) entityId.getComponentByIndex(0)) },
          false);
    case FORMATTED:
      final HashSpec hashSpec = ((RowKeyFormat2) keysFormat).getSalt();
      if (!hashSpec.getSuppressKeyMaterialization()) {
        return new KijiRestEntityId(entityId.getComponents().toArray(), false);
      } else {
        return new KijiRestEntityId(
            String.format("hbase=%s", Bytes.toStringBinary(entityId.getHBaseRowKey())));
      }
    default:
      return new KijiRestEntityId(
          String.format("hbase=%s", Bytes.toStringBinary(entityId.getHBaseRowKey())));
    }
  }

  /**
   * Gets row key encoding of a row key format.
   *
   * @param keysFormat row key format.
   * @return row key encoding.
   * @throws IOException if row key format is unrecognized.
   */
  private static RowKeyEncoding getEncoding(final Object keysFormat) throws IOException {
    if (keysFormat instanceof RowKeyFormat) {
      return ((RowKeyFormat) keysFormat).getEncoding();
    } else if (keysFormat instanceof RowKeyFormat2) {
      return ((RowKeyFormat2) keysFormat).getEncoding();
    } else {
      throw new IOException(
          String.format("Unrecognized row key format: %s", keysFormat.getClass()));
    }
  }

  /**
   * Create KijiRestEntityId from json node.
   *
   * @param node of the RKF2-formatted, materialization unsuppressed row.
   * @return a properly constructed KijiRestEntityId.
   * @throws IOException if KijiRestEntityId can not be properly constructed.
   */
  public static KijiRestEntityId create(final JsonNode node) throws IOException {
    return create(node, (RowKeyFormat2) null);
  }

  /**
   * Gets the RowKeyFormat2 of the provided layout, if it exists. Otherwise, null.
   *
   * @param layout of the table to find the RowKeyFormat2.
   * @return the RowKeyFormat2, null if the layout has RowKeyFormat1.
   * @throws IOException if the keys format can not be ascertained.
   */
  private static RowKeyFormat2 getRKF2(final KijiTableLayout layout) throws IOException {
    if (null != layout
        && RowKeyEncoding.FORMATTED == getEncoding(layout.getDesc().getKeysFormat())) {
      return (RowKeyFormat2) layout.getDesc().getKeysFormat();
    } else {
      return null;
    }
  }

  /**
   * Create KijiRestEntityId from json node.
   *
   * @param node of the RKF2-formatted, materialization unsuppressed row.
   * @param layout of the table in which the entity id belongs.
   *        If null, then long components may not be recognized.
   * @return a properly constructed KijiRestEntityId.
   * @throws IOException if KijiRestEntityId can not be properly constructed.
   */
  public static KijiRestEntityId create(
      final JsonNode node,
      final KijiTableLayout layout) throws IOException {
    return create(node, getRKF2(layout));
  }

  /**
   * Create KijiRestEntityId from json node.
   *
   * @param node of the RKF2-formatted, materialization unsuppressed row.
   * @param rowKeyFormat2 of the layout or null if the layout has RowKeyFormat1.
   *        If null, then long components may not be recognized.
   * @return a properly constructed KijiRestEntityId.
   * @throws IOException if KijiRestEntityId can not be properly constructed.
   */
  public static KijiRestEntityId create(
      final JsonNode node,
      final RowKeyFormat2 rowKeyFormat2) throws IOException {
    Preconditions.checkNotNull(node);
    if (node.isArray()) {
      final Object[] components = new Object[node.size()];
      boolean wildCarded = false;
      for (int i = 0; i < node.size(); i++) {
        final Object component = getNodeValue(node.get(i));
        if (component.equals(WildcardSingleton.INSTANCE)) {
          wildCarded = true;
          components[i] = null;
        } else if (null != rowKeyFormat2
            && ComponentType.LONG == rowKeyFormat2.getComponents().get(i).getType()) {
          components[i] = ((Number) component).longValue();
        } else {
          components[i] = component;
        }
      }
      return new KijiRestEntityId(components, wildCarded);
    } else {
      // Disallow non-arrays.
      throw new IllegalArgumentException(
          "Provide components wrapped as a JSON array or provide the row key.");
    }
  }

  /**
   * Gets the array of components.
   *
   * @return array of components.
   */
  public Object[] getComponents() {
    return mComponents;
  }

  /**
   * Are any of the components wildcarded...
   *
   * @return true iff at least one component is a wildcard (indicated by a null).
   */
  public boolean isWildcarded() {
    return mIsWildcarded;
  }

  /**
   * Gets the json node eid (which can be null if the eid was backed as a string).
   *
   * @return json node of the eid.
   */
  public JsonNode getJsonEntityId() {
    return BASIC_MAPPER.valueToTree(mComponents);
  }

  /**
   * Gets the string representation of the eid.
   *
   * @return string representation of eid.
   */
  public String getStringEntityId() {
    return mStringEntityId;
  }

  /**
   * If the eid backed by a json or a string?
   *
   * @return true iff eid is backed by json node.
   */
  public boolean hasComponents() {
    return mComponents != null;
  }

  /**
   * Construct eid from a entity id string.
   * Formatted entity ids mustn't have wildcards in order to resolve.
   *
   * @param layout of table in which to construct the eid.
   * @return the eid.
   * @throws IOException if construction of eid fails due to incorrect user input.
   */
  public EntityId resolve(final KijiTableLayout layout) throws IOException {
    if (this.hasComponents()) {
      if (this.isWildcarded()) {
        throw new IllegalArgumentException(
            "Entity id must be fully specified for resolution, i.e. without wildcards.");
      }
      final RowKeyEncoding encoding = getEncoding(layout.getDesc().getKeysFormat());
      switch (encoding) {
      case FORMATTED:
        return EntityIdFactory.getFactory(layout).getEntityId(mComponents);
      default:
        return EntityIdFactory.getFactory(layout).getEntityId(mComponents[0]);
      }
    } else {
      final EntityIdFactory factory = EntityIdFactory.getFactory(layout);
      return factory.getEntityIdFromHBaseRowKey(parseBytes(mStringEntityId));
    }
  }

  /**
   * Gets byte array from string entity id given in "hbase=" or "hbase_hex" format.
   *
   * @param stringEntityId representing the row to acquire byte array for.
   * @return byte array of entity id.
   * @throws IOException if the ASCII-encoded hex was improperly formed.
   */
  private static byte[] parseBytes(final String stringEntityId) throws IOException {
    if (stringEntityId.startsWith(HBASE_ROW_KEY_PREFIX)) {
      final String rowKeySubstring = stringEntityId.substring(HBASE_ROW_KEY_PREFIX.length());
      return Bytes.toBytesBinary(rowKeySubstring);
    } else if (stringEntityId.startsWith(HBASE_HEX_ROW_KEY_PREFIX)) {
      final String rowKeySubstring = stringEntityId.substring(HBASE_HEX_ROW_KEY_PREFIX.length());
      try {
        return Hex.decodeHex(rowKeySubstring.toCharArray());
      } catch (DecoderException de) {
        // Re-wrap decoder exception as IOException.
        throw new IOException(de.getMessage());
      }
    } else {
      throw new IllegalArgumentException("Passed string must be prefixed by hbase= or hbase_hex=.");
    }
  }

  /**
   * Converts a JSON string, integer, or wildcard (empty array)
   * node into a Java object (String, Integer, Long, WILDCARD, or null).
   *
   * @param node JSON string, integer numeric, or wildcard (empty array) node.
   * @return the JSON value, as a String, an Integer, a Long, a WILDCARD, or null.
   * @throws JsonParseException if the JSON node is not String, Integer, Long, WILDCARD, or null.
   */
  private static Object getNodeValue(JsonNode node) throws JsonParseException {
    // TODO: Write tests to distinguish integer and long components.
    if (node.isInt()) {
      return node.asInt();
    } else if (node.isLong()) {
      return node.asLong();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isArray() && node.size() == 0) {
      // An empty array token indicates a wildcard.
      return WildcardSingleton.INSTANCE;
    } else if (node.isNull()) {
      return null;
    } else {
      throw new JsonParseException(String.format(
          "Invalid JSON value: '%s', expecting string, int, long, null, or wildcard [].", node),
          null);
    }
  }

  /**
   * Singleton object to use to represent a wildcard.
   */
  private static enum WildcardSingleton {
    INSTANCE;
  }

  @Override
  public String toString() {
    if (this.hasComponents()) {
      return this.getJsonEntityId().toString();
    } else {
      return this.getStringEntityId();
    }
  }
}
