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

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
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
   * Saves a freshness policy in the metatable using classes.
   *
   * @param tableName the table name with which the freshness policy should be associated. If the
   * table doesn't exist, an IOException will be thrown.
   * @param columnName the name of the column to associate the freshness policy with. This may be
   * either in the format of "family:qualifier" for a single column or "family" for an entire
   * map family.
   * @param producerClass class of the producer to run if the data in the column is not fresh.
   * @param policy an instance of a KijiFreshnessPolicy that will manage freshening behavior.
   * @throws IOException if there is an error saving the freshness policy or if the table is not
   * found.
   */
  public void storePolicy(String tableName, String columnName,
      Class<? extends KijiProducer> producerClass, KijiFreshnessPolicy policy)
      throws IOException {
    // Check that the table exists.
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }
    // Check that the column name is valid.
    KijiColumnName kcn = new KijiColumnName(columnName);
    // Check that the table includes the specified column or family.
    final KijiTable table = mKiji.openTable(tableName);
    final Set<KijiColumnName> columnsNames = table.getLayout().getColumnNames();
    final Map<String, FamilyLayout> familyMap = table.getLayout().getFamilyMap();
    // Check that the column exists if it is fully qualified
    if (kcn.isFullyQualified()) {
      if (familyMap.containsKey(kcn.getFamily()) && familyMap.get(kcn.getFamily()).isGroupType()
          && !columnsNames.contains(kcn)) {
        throw new IllegalArgumentException(String.format(
            "Table does not contain specified column: %s", kcn.toString()));
      } else {
        if (familyMap.containsKey(kcn.getFamily()) && familyMap.get(kcn.getFamily()).isMapType()
            && mMetaTable.keySet(tableName).contains(getMetaTableKey(kcn.getFamily()))) {
          // check for a policy attached to kcn.getFamily()
          throw new IllegalArgumentException(String.format("There is already a freshness policy "
              + "attached to family: %s Freshness policies may not be attached to a map type "
              + "family and fully qualified columns within that family.", kcn.getFamily()));
        }
      }
    } else {
      // If not fully qualified, check that the family exists and is a map type family.
      if (!familyMap.containsKey(kcn.toString()) || familyMap.get(kcn.toString()).isGroupType()) {
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
    // TODO(Score-22): Design checks to perform here.
    // currently checks that the table exists, that the column name is valid, that the column
    // exists if it is fully qualified, that the family is a map type family if not fully
    // qualified, and that the new attachment does not violate the exclusion between family level
    // freshening and fully qualified freshening in a map type family.

    // Once all checks have passed, register the strings directly.
    storePolicyWithStrings(
        tableName,
        columnName,
        producerClass.getName(),
        policy.getClass().getName(),
        policy.serialize());
  }

  /**
   * Saves a freshness policy in the metatable without performing any checks on compatibility of
   * components.
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
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }
    // This code will throw an invalid name if there's something wrong with this columnName string.
    KijiColumnName kcn = new KijiColumnName(columnName);
    //TODO(Scoring-10): Check the column name against the current version of the table.
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
    mMetaTable.removeValues(tableName, getMetaTableKey(columnName));
  }

  /**
   * Remove all freshness policies for a given table.
   *
   * @param tableName the table from which to remove all freshness policies.
   * @throws IOException in case of an error reading from the metatable.
   */
  public void removePolicies(String tableName) throws IOException {
    final Set<String> keys = mMetaTable.keySet(tableName);
    for (String key : keys) {
      if (key.startsWith(METATABLE_KEY_PREFIX)) {
        mMetaTable.removeValues(tableName, key);
      }
    }
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
}
