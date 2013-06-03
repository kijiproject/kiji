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

package org.kiji.scoring;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * This class is responsible for storing, retrieving and deleting freshness policies from a Kiji's
 * metatable.
 *
 * <p>Instances of this class are not thread-safe. Since this class maintains a connection to the
 * metatable clients should call {@link #close()} when finished with this class.</p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class KijiFreshnessManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiFreshnessManager.class);

  /** The minimum freshness version supported by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion MIN_FRESHNESS_RECORD_VER =
      ProtocolVersion.parse("policyrecord-0.1");
  /** Maximum freshness version supported by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion MAX_FRESHNESS_RECORD_VER =
      ProtocolVersion.parse("policyrecord-0.1");
  /** The freshness version that will be installed by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion CUR_FRESHNESS_RECORD_VER = MAX_FRESHNESS_RECORD_VER;

  /** The prefix we use for freshness policies stored in a meta table. */
  private static final String METATABLE_KEY_PREFIX = "kiji.scoring.fresh.";

  /** The Kiji instance to manage freshness policies within. */
  private final Kiji mKiji;

  /** The backing metatable. */
  private final KijiMetaTable mMetaTable;

  /** An output stream writer suitable for serializing FreshnessPolicyRecords. */
  private final ByteArrayOutputStream mOutputStream;
  /** A datum writer for records. */
  private final DatumWriter<KijiFreshnessPolicyRecord> mRecordWriter;
  /** An encoder factory for serializing records. */
  private final EncoderFactory mEncoderFactory;
  /** An input stream reader suitable for deserializing FreshnessPolicyRecords. */
  /** A datum reader for records. */
  private final DatumReader<KijiFreshnessPolicyRecord> mRecordReader;
  /** A decoder factory for records. */
  private final DecoderFactory mDecoderFactory;

  /**
   * Default constructor.
   *
   * @param kiji a Kiji instance containing the metatable storing freshness information.
   * @throws IOException if there is an error retrieving the metatable.
   */
  private KijiFreshnessManager(Kiji kiji) throws IOException {
    mKiji = kiji;
    mMetaTable = kiji.getMetaTable();
    // Setup members responsible for serializing/deserializing records.
    mOutputStream = new ByteArrayOutputStream();
    mRecordWriter =
        new SpecificDatumWriter<KijiFreshnessPolicyRecord>(KijiFreshnessPolicyRecord.SCHEMA$);
    mRecordReader =
        new SpecificDatumReader<KijiFreshnessPolicyRecord>(KijiFreshnessPolicyRecord.SCHEMA$);
    mEncoderFactory = EncoderFactory.get();
    mDecoderFactory = DecoderFactory.get();
  }

  /**
   * Create a new KijiFreshnessManager for the given Kiji instance.
   *
   * @param kiji the Kiji instance for which to create a freshness manager.
   * @return a new KijiFreshnessManager.
   * @throws IOException in case of an error reading from the KijiMetaTable.
   */
  public static KijiFreshnessManager create(Kiji kiji) throws IOException {
    return new KijiFreshnessManager(kiji);
  }

  /**
   * Saves a freshness policy in the metatable using classes.  If possible, this method will ensure
   * that the target attachment column and the producer's output column are compatible (both
   * unqualified map type families, or both fully qualified columns) and that the producer's data
   * request can be fulfilled by the target table.  If these checks pass, further checks will be
   * performed according to {@link #storePolicyWithStrings(String, String, String, String, String)}.
   *
   * @param tableName the table name with which the freshness policy should be associated. If the
   * table doesn't exist, an IOException will be thrown.
   * @param columnName the name of the column to associate the freshness policy with. This may be
   * either in the format of "family:qualifier" for a single column or "family" for an entire
   * map family.
   * @param producerClass class of the producer to run if the data in the column is not fresh.
   * @param policy an instance of a KijiFreshnessPolicy that will manage freshening behavior.  This
   * object's serialize method will be called and the return value used as the policy's saved state.
   * @throws IOException if there is an error saving the freshness policy or if the table is not
   * found.
   */
  public void storePolicy(String tableName, String columnName,
      Class<? extends KijiProducer> producerClass, KijiFreshnessPolicy policy)
      throws IOException {

    // This code will throw an invalid name if there's something wrong with this columnName string.
    KijiColumnName kcn = new KijiColumnName(columnName);
    final KijiProducer producer = ReflectionUtils.newInstance(producerClass, null);

    // Check that the column name to attach to matches the type of the column name to which the
    // producer was designed to write.
    if (producer.getOutputColumn() != null) {
      final KijiColumnName outputColumn = new KijiColumnName(producer.getOutputColumn());
      Preconditions.checkArgument(kcn.isFullyQualified() == outputColumn.isFullyQualified(),
          "Producer output column and attachment column qualifications do not agree.  Both must be "
          + "either unqualified or fully qualified.  Producer column: %s Attachment column: %s",
          producer.getOutputColumn(), columnName);
      // if outputColumn is null, we cannot validate.
    }

    // validate that data request can be fulfilled by the table.
    KijiDataRequest producerRequest = null;
    try {
      producerRequest = producer.getDataRequest();
    } catch (NullPointerException npe) {
      LOG.debug("NullPointerException thrown by producer.getDataRequest().  Cannot perform "
          + "validation.  Please ensure that your producer is designed to write to the same type "
          + "of column to which your freshness policy is attached.");
    }
    if (producerRequest != null) {
      final KijiTableLayout layout = mKiji.openTable(tableName).getLayout();
      for (Column column : producerRequest.getColumns()) {
        final KijiColumnName name = new KijiColumnName(column.getFamily(), column.getQualifier());
        Preconditions.checkArgument(layout.exists(name), "Column: %s in producer data request "
              + "does not exist in table: %s", name.toString(), tableName);
      }
      // The data request may be null if it is configured using KVStores.  If it is null, we
      // cannot validate.
    }

    // Collect the appropriate strings from the objects and write them with storePolicyWithStrings.
    storePolicyWithStrings(
        tableName,
        columnName,
        producerClass.getName(),
        policy.getClass().getName(),
        policy.serialize());
  }

  /**
   * Checks if a String is a valid Java class identifier.
   * @param className the identifier to check.
   * @return whether the string is a valid Java class identifier.
   */
  private boolean isValidClassName(String className) {
    if (className.endsWith(".")) {
      return false;
    }
    final String[] splitId = className.split("\\.");
    for (String section : splitId) {
      final char[] chars = section.toCharArray();
      // An empty section indicates leading or consecutive '.'s which are illegal.
      if (chars.length == 0) {
        return false;
      }
      if (!Character.isJavaIdentifierStart(chars[0])) {
        return false;
      }
      for (int i = 1; i < chars.length; i++) {
        if (!Character.isJavaIdentifierPart(chars[i])) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Saves a freshness policy in the metatable without requiring classes to be available on the
   * classpath.  This method ensures that producer and policy class names are valid Java class
   * identifiers, that the column exists in the table layout if it is a fully qualified member of a
   * group type family, that the family exists if the specified columnName is not fully qualified,
   * and that there is not a conflict between existing freshness policies and the new policy to
   * attach (i.e. there may never be two freshness policies associated with the same fully qualified
   * column.).
   *
   * @param tableName the table name with which the freshness policy should be associated.  Throws
   * an IOException if the table does not exist.
   * @param columnName the name of the column with which to associate the freshness policy.  This
   * may be either a fully qualified column from a group type family, or an unqualified map type
   * family name.
   * @param producerClass the fully qualified class name of the producer to use for freshening.
   * @param policyClass the fully qualified class name of the KijiFreshnessPolicy to attach to the
   * column.
   * @param policyState the serialized state of the policy class.
   * @throws IOException in case of an error writing to the metatable.
   */
  public void storePolicyWithStrings(String tableName, String columnName, String producerClass,
      String policyClass, String policyState) throws IOException {
    // Check that the table exists.
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }
    // Check that the policy class name is a valid Java identifier.
    if (!isValidClassName(policyClass)) {
      throw new IllegalArgumentException(String.format(
          "Policy class name: %s is not a valid Java class identifier.", policyClass));
    }
    // Check that the producer class name is a valid Java identifier.
    if (!isValidClassName(producerClass)) {
      throw new IllegalArgumentException(String.format(
          "Producer class name: %s is not a valid Java class identifier.", producerClass));
    }
    // This code will throw an invalid name if there's something wrong with this columnName string.
    KijiColumnName kcn = new KijiColumnName(columnName);
    // Check that the table includes the specified column or family.
    final KijiTable table = mKiji.openTable(tableName);
    final Set<KijiColumnName> columnsNames = table.getLayout().getColumnNames();
    final Map<String, FamilyLayout> familyMap = table.getLayout().getFamilyMap();

    // Check that the family exists in the table layout.
    if (!familyMap.containsKey(kcn.getFamily())) {
      throw new IllegalArgumentException(String.format(
          "Table: %s does not contain family: %s", tableName, kcn.getFamily()));
    }

    // Check that the column exists if it is fully qualified
    if (kcn.isFullyQualified()) {
      if (familyMap.get(kcn.getFamily()).isGroupType()) {
        // Check that the fully qualified group family column exists in the table layout.
        if (!columnsNames.contains(kcn)) {
          throw new IllegalArgumentException(String.format(
              "Table: %s does not contain specified column: %s", tableName, kcn.toString()));
        }
      } else {
        // Check for a policy attached to kcn.getFamily()
        if (mMetaTable.keySet(tableName).contains(getMetaTableKey(kcn.getFamily()))) {
          throw new IllegalArgumentException(String.format("There is already a freshness policy "
              + "attached to family: %s Freshness policies may not be attached to a map type "
              + "family and fully qualified columns within that family.", kcn.getFamily()));
        }
      }
    } else {
      // If not fully qualified, check that the family is a map type family.
      if (!familyMap.get(kcn.toString()).isMapType()) {
        throw new IllegalArgumentException(String.format(
            "Specified family: %s is not a valid Map Type family in the table: %s",
            kcn.toString(), tableName));
      } else {
        // check for a policy attached to any qualified column in kcn
        final Set<String> keys = mMetaTable.keySet(tableName);
        boolean qualifiedColumnExists = false;
        for (String key : keys) {
          final boolean keyDisqualifies = key.startsWith(getMetaTableKey(kcn.toString()));
          if (keyDisqualifies) {
            LOG.error("Cannot attach freshness policy to family: {} qualified column: {} already "
                + "has an attached freshness policy.", kcn.toString(),
                key.substring(METATABLE_KEY_PREFIX.length()));
          }
          qualifiedColumnExists = keyDisqualifies || qualifiedColumnExists;

        }
        if (qualifiedColumnExists) {
          throw new IllegalArgumentException(String.format("There is already a freshness policy "
              + "attached to a fully qualified column in family: %s Freshness policies may not be "
              + "attached to a map type family and fully qualified columns within that family. To "
              + "view a list of attached freshness policies check log files for "
              + "KijiFreshnessManager.",
              kcn.toString()));
        }
      }
    }

    KijiFreshnessPolicyRecord record = KijiFreshnessPolicyRecord.newBuilder()
        .setRecordVersion(CUR_FRESHNESS_RECORD_VER.toCanonicalString())
        .setProducerClass(producerClass)
        .setFreshnessPolicyClass(policyClass)
        .setFreshnessPolicyState(policyState)
        .build();

    mOutputStream.reset();
    Encoder encoder = mEncoderFactory.directBinaryEncoder(mOutputStream, null);
    mRecordWriter.write(record, encoder);
    mMetaTable.putValue(tableName, getMetaTableKey(columnName),
        mOutputStream.toByteArray());
  }

  /**
   * Retrieves a freshness policy record for a tablename and column. Will return null if there is
   * no freshness policy registered for that column.
   *
   * @param tableName the table name.
   * @param columnName the column name, represented as a String in the form of "family:qualifier" or
   * "family" for a map family.
   * @return an avro KijiFreshnessPolicyRecord.
   * @throws IOException if an error occurs while reading from the metatable.
   */
  public KijiFreshnessPolicyRecord retrievePolicy(
      String tableName, String columnName) throws IOException {
    final byte[] recordBytes;
    // TODO(Schema-341) stop catching IOException and replace with checking for null.
    // recordBytes = mMetaTable.getValue(tableName, getMetaTableKey(columnName));
    try {
      recordBytes = mMetaTable.getValue(tableName, getMetaTableKey(columnName));
    } catch (IOException ioe) {
      if (ioe.getMessage().equals(String.format(
          "Could not find any values associated with table %s and key %s", tableName,
          getMetaTableKey(columnName)))) {
        return null;
      } else {
        throw ioe;
      }
    }
    Decoder decoder = mDecoderFactory.binaryDecoder(recordBytes, null);
    return mRecordReader.read(null, decoder);
  }

  /**
   * Retrieves all freshness policy records for a table.  Will return an empty map if there are no
   * freshness policies registered for that table.
   *
   * @param tableName the table name.
   * @return a Map from KijiColumnNames to attached KijiFreshnessPolicyRecords.
   * @throws IOException if an error occurs while reading from the metatable.
   */
  public Map<KijiColumnName, KijiFreshnessPolicyRecord> retrievePolicies(String tableName)
      throws IOException {
    final Set<String> keySet = mMetaTable.keySet(tableName);
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> records =
        new HashMap<KijiColumnName, KijiFreshnessPolicyRecord>();
    for (String key: keySet) {
      if (key.startsWith(METATABLE_KEY_PREFIX)) {
        final String columnName = key.substring(METATABLE_KEY_PREFIX.length());
        records.put(new KijiColumnName(columnName), retrievePolicy(tableName, columnName));
      }
    }
    return records;
  }

  /**
   * Unregisters a policy.
   *
   * @param tableName the table name.
   * @param columnName The column name, represented as a String in the form of "family:qualifier" or
   * "family" for a map family.
   * @throws IOException if an error occurs while deregistering in the metatable.
   */
  public void removePolicy(String tableName, String columnName) throws IOException {
    // Check that the table exists.
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }
    // This code will throw an invalid name if there's something wrong with this columnName string.
    KijiColumnName kcn = new KijiColumnName(columnName);

    Preconditions.checkArgument(mMetaTable.keySet(tableName).contains(getMetaTableKey(columnName)),
        "There is no freshness policy attached to column: %s in table: %s", columnName, tableName);

    mMetaTable.removeValues(tableName, getMetaTableKey(columnName));
  }

  /**
   * Remove all freshness policies for a given table.
   *
   * @param tableName the table from which to remove all freshness policies.
   * @return the set of columns from which freshness policies were removed.  Returns an empty set if
   * no freshness policies are found.
   * @throws IOException in case of an error reading from the metatable.
   */
  public Set<KijiColumnName> removePolicies(String tableName) throws IOException {
    // Check that the table exists.
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }

    final Set<String> keys = mMetaTable.keySet(tableName);
    final Set<KijiColumnName> removedColumns = Sets.newHashSet();
    for (String key : keys) {
      if (key.startsWith(METATABLE_KEY_PREFIX)) {
        mMetaTable.removeValues(tableName, key);
        removedColumns.add(fromMetaTableKey(key));
      }
    }
    return removedColumns;
  }

  /**
   * Closes the manager, freeing resources.
   *
   * @throws IOException if an error occurs.
   */
  public void close() throws IOException {
    mMetaTable.close();
  }

  /**
   * Helper method that constructs a meta table key for a column name.
   *
   * @param columnName the column for which to get a MetaTable key.
   * @return the MetaTable key for the given column.
   */
  private String getMetaTableKey(String columnName) {
    return METATABLE_KEY_PREFIX + columnName;
  }

  /**
   * Helper method that constructs a KijiColumnNAme from a meta table key.
   *
   * @param metaTableKey the meta table key from which to get a column name.
   * @return the KijiColumnName from the given key.
   */
  private KijiColumnName fromMetaTableKey(String metaTableKey) {
    return new KijiColumnName(metaTableKey.substring(METATABLE_KEY_PREFIX.length()));
  }
}
