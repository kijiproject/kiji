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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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

  public static final Map<String, String> DEFAULT_PARAMETERS = Collections.emptyMap();
  public static final boolean DEFAULT_REINITIALIZE = false;

  /** The minimum freshness version supported by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion MIN_FRESHNESS_RECORD_VER =
      ProtocolVersion.parse("policyrecord-0.1");
  /** Maximum freshness version supported by this version of the KijiFreshnessManager. */
  private static final ProtocolVersion MAX_FRESHNESS_RECORD_VER =
      ProtocolVersion.parse("policyrecord-0.2");
  /** The freshness version that will be installed by this version of the KijiFreshnessManager. */
  public static final ProtocolVersion CUR_FRESHNESS_RECORD_VER = MAX_FRESHNESS_RECORD_VER;
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
    // Retain the Kiji instance only if the rest of the constructor has succeeded.
    mKiji.retain();
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
  public void storePolicy(
      String tableName,
      String columnName,
      Class<? extends KijiProducer> producerClass,
      KijiFreshnessPolicy policy
  ) throws IOException {
    storePolicy(
        tableName,
        columnName,
        producerClass,
        policy,
        DEFAULT_PARAMETERS,
        DEFAULT_REINITIALIZE);
  }

  /**
   * Saves a freshness policy in the metatable using classes.  If possible, this method will ensure
   * that the target attachment column and the producer's output column are compatible (both
   * unqualified map type families, or both fully qualified columns) and that the producer's data
   * request can be fulfilled by the target table.  If these checks pass, further checks will be
   * performed according to {@link #storePolicyWithStrings(String, String, String, String, String,
   * java.util.Map, boolean)}.
   *
   * @param tableName the table name with which the freshness policy should be associated. If the
   *     table doesn't exist, an IOException will be thrown.
   * @param columnName the name of the column to associate the freshness policy with. This may be
   * either in the format of "family:qualifier" for a single column or "family" for an entire
   *     map family.
   * @param producerClass class of the producer to run if the data in the column is not fresh.
   * @param policy an instance of a KijiFreshnessPolicy that will manage freshening behavior.  This
   *     object's serialize method will be called and the return value used as the policy's saved
   *     state.
   * @param parameters a string-string mapping of configuration parameters which will be available
   *     to the FreshnessPolicy and KijiProducer.
   * @param reinitializeProducer whether to reinitialize the KijiProducer for each request. This
   *     value may be overridden by
   *     {@link org.kiji.scoring.PolicyContext#reinitializeProducer(boolean)}.
   * @throws IOException if there is an error saving the freshness policy or if the table is not
   *     found.
   */
  public void storePolicy(
      final String tableName,
      final String columnName,
      final Class<? extends KijiProducer> producerClass,
      final KijiFreshnessPolicy policy,
      final Map<String, String> parameters,
      final boolean reinitializeProducer
  ) throws IOException {
    final Map<ValidationFailure, Exception> failures =
        validateAttachment(tableName, columnName, producerClass.getName(),
            policy.getClass().getName(), true);

    if (failures.isEmpty()) {
      // Collect the appropriate strings from the objects and write them with storePolicyWithStrings
      storePolicyWithStrings(
          tableName,
          columnName,
          producerClass.getName(),
          policy.getClass().getName(),
          policy.serialize(),
          parameters,
          reinitializeProducer);
    } else {
      throw new FreshnessValidationException(failures);
    }
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
  public void storePolicyWithStrings(
      String tableName,
      String columnName,
      String producerClass,
      String policyClass,
      String policyState
  ) throws IOException {
    storePolicyWithStrings(
        tableName,
        columnName,
        producerClass,
        policyClass,
        policyState,
        DEFAULT_PARAMETERS,
        DEFAULT_REINITIALIZE);
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
   * @param parameters configuration parameters which will be available to the FreshnessPolicy and
   *     KijiProducer.
   * @param reinitializeProducer whether to reinitialize the KijiProducer on each request.  This
   *     value may be overridden by the FreshnessPolicy using
   *     {@link org.kiji.scoring.PolicyContext#reinitializeProducer(boolean)}.
   * @throws IOException in case of an error writing to the metatable.
   */
  public void storePolicyWithStrings(
      String tableName,
      String columnName,
      String producerClass,
      String policyClass,
      String policyState,
      Map<String, String> parameters,
      boolean reinitializeProducer
  ) throws IOException {
    final Map<ValidationFailure, Exception> failures =
        validateAttachment(tableName, columnName, producerClass, policyClass, true);

    if (failures.isEmpty()) {
      KijiFreshnessPolicyRecord record = KijiFreshnessPolicyRecord.newBuilder()
          .setRecordVersion(CUR_FRESHNESS_RECORD_VER.toCanonicalString())
          .setProducerClass(producerClass)
          .setFreshnessPolicyClass(policyClass)
          .setFreshnessPolicyState(policyState)
          .setParameters(parameters)
          .setReinitializeProducer(reinitializeProducer)
          .build();

      writeRecordToMetaTable(tableName, columnName, record);
    } else {
      throw new FreshnessValidationException(failures);
    }
  }


  /**
   * Attach several freshness policies to columns in the given table.
   *
   * @param tableName the name of the table in which to attach freshness policies.
   * @param records a mapping from columns to records which will be attached to those columns.
   * @param overwriteExisting whether or not to overwrite existing freshness policy records.
   * @throws IOException in case of an error writing to the metatable.
   */
  public void storePolicies(
      final String tableName,
      final Map<KijiColumnName, KijiFreshnessPolicyRecord> records,
      final boolean overwriteExisting
  ) throws IOException {
    final Set<String> keySet = mMetaTable.keySet(tableName);
    final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry : records.entrySet()) {
      final String metatableKey = getMetaTableKey(entry.getKey());
      if (keySet.contains(metatableKey) && overwriteExisting) {
        mMetaTable.removeValues(tableName, metatableKey);
      }
      final Map<ValidationFailure, Exception> individualFailures = validateAttachment(
          tableName,
          entry.getKey().getName(),
          entry.getValue().getProducerClass(),
          entry.getValue().getFreshnessPolicyClass(),
          true);
      if (!individualFailures.isEmpty()) {
        failures.put(entry.getKey(), individualFailures);
      }
    }
    if (!failures.isEmpty()) {
      throw new MultiFreshnessValidationException(failures);
    } else {
      for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry : records.entrySet()) {
        writeRecordToMetaTable(tableName, entry.getKey().toString(), entry.getValue());
      }
    }
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
    for (String key : keySet) {
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

  /** Enumeration of possible Validation failure causes. */
  public static enum ValidationFailure {
    BAD_POLICY_NAME,
    BAD_PRODUCER_NAME,
    NO_FAMILY_IN_TABLE,
    NO_QUALIFIED_COLUMN_IN_TABLE,
    FRESHENER_ALREADY_ATTACHED,
    GROUP_TYPE_FAMILY_ATTACHMENT,
    PRODUCER_OUTPUT_COLUMN_DOES_NOT_MATCH,
    PRODUCER_REQUEST_CANNOT_BE_FULFILLED
  }

  /**
   * An aggregate exception representing one or several validation failures for a single request.
   */
  public static final class FreshnessValidationException extends RuntimeException {
    /** A map of validation failures represented collectively by this excpetion. */
    private final Map<ValidationFailure, Exception> mFailures;

    /**
     * Construct a new FreshnessValidationException from a map of validation failures.
     *
     * @param failures a map of validation failures with which to build this exception.
     */
    public FreshnessValidationException(Map<ValidationFailure, Exception> failures) {
      Preconditions.checkNotNull(failures, "Cannot build a FreshnessValidationException from a null"
          + " failure map.");
      Preconditions.checkArgument(!failures.isEmpty(), "Cannot build a FreshnessValidationException"
          + " from an empty map.");
      mFailures = failures;
    }

    /** {@inheritDoc} */
    @Override
    public String getMessage() {
      final StringBuilder builder = new StringBuilder();
      builder.append("There were validation failures.");
      for (Map.Entry<ValidationFailure, Exception> entry : mFailures.entrySet()) {
        builder.append(String.format(
            "%n%s: %s", entry.getKey().toString(), entry.getValue().getMessage()));
      }
      return builder.toString();
    }

    /**
     * Get the map from {@link ValidationFailure} to Exception that was used to construct this
     * exception.
     *
     * @return the map from {@link ValidationFailure} to Exception that was used to construct this
     * exception.
     */
    public Map<ValidationFailure, Exception> getExceptions() {
      return mFailures;
    }
  }

  /** An aggregate exception representing validation failures for multiple columns. */
  public static final class MultiFreshnessValidationException extends RuntimeException {

    private final Map<KijiColumnName, Map<ValidationFailure, Exception>> mFailures;

    /**
     * Construct a new  MultiFreshnessValidationException from a map of validation failures.
     *
     * @param failures a map of validation failure with which to build this exception.
     */
    public MultiFreshnessValidationException(
        final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures
    ) {
      Preconditions.checkNotNull(failures, "Cannot build a MultiFreshnessValidationException from a"
          + " null failure map.");
      Preconditions.checkArgument(!failures.isEmpty(), "Cannot build a "
          + "MultiFreshnessValidationException from an empty failure map.");
      mFailures = failures;
    }

    /** {@inheritDoc} */
    @Override
    public String getMessage() {
      final StringBuilder builder = new StringBuilder();
      builder.append("There were validation failures.");
      for (Map.Entry<KijiColumnName, Map<ValidationFailure, Exception>> entry
          : mFailures.entrySet()) {
        builder.append(String.format("%n%s:", entry.getKey()));
        for (Map.Entry<ValidationFailure, Exception> innerEntry : entry.getValue().entrySet()) {
          builder.append(String.format("%n%s: %s",
              innerEntry.getKey().toString(), innerEntry.getValue().getMessage()));
        }
      }
      return builder.toString();
    }

    /**
     * Get the map from attached column to ValidationFailure to Exception that was used to construct
     * this exception.
     *
     * @return the map from attached column to ValidationFailure to Exception that was used to
     * construct this exception.
     */
    public Map<KijiColumnName, Map<ValidationFailure, Exception>> getExceptions() {
      return mFailures;
    }
  }

  /**
   * Validate that a freshness policy attached to the given column in the given table conforms to
   * the restrictions imposed on attachment.
   *
   * @param tableName the name of the table containing the target column.
   * @param columnName the name of the column whose freshness policy should be validated.
   * @return a map from {@link ValidationFailure} type to validation exception.  This map is
   * populated with items only for failed validation checks.  An empty map indicates no validation
   * failures.
   * @throws IOException in case of an error reading from the metatable.
   */
  public Map<ValidationFailure, Exception> validatePolicy(String tableName, String columnName)
      throws IOException {
    // Check that the table exists.
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }
    // This code will throw an invalid name if there's something wrong with this columnName string.
    KijiColumnName kcn = new KijiColumnName(columnName);

    final Map<ValidationFailure, Exception> failures = new HashMap<ValidationFailure, Exception>();

    byte[] recordBytes = null;
    try {
      recordBytes = mMetaTable.getValue(tableName, getMetaTableKey(columnName));
    } catch (IOException ioe) {
      if (ioe.getMessage().equals(String.format(
          "Could not find any values associated with table %s and key %s", tableName,
          getMetaTableKey(columnName)))) {
        throw new IllegalArgumentException(String.format("No freshness policy attached to column: "
            + "%s in table: %s", columnName, tableName));
      } else {
        throw ioe;
      }
    }
    Decoder decoder = mDecoderFactory.binaryDecoder(recordBytes, null);
    final KijiFreshnessPolicyRecord record = mRecordReader.read(null, decoder);

    try {
      final KijiProducer producer = ReflectionUtils.newInstance(
          Class.forName(record.getProducerClass()).asSubclass(KijiProducer.class), null);

      // Check that the column name to attach to matches the type of the column name to which the
      // producer was designed to write.
      KijiColumnName outputColumn = null;
      try {
        outputColumn = new KijiColumnName(Preconditions.checkNotNull(producer.getOutputColumn()));
      } catch (NullPointerException npe) {
        LOG.debug("NullPointerException thrown by producer.getOutputColumn().  Cannot perform "
            + "validation.  Please ensure that your producer is designed to write to the same type "
            + "of column to which your freshness policy is attached.");
      }

      if (outputColumn != null) {
        if (kcn.isFullyQualified() != outputColumn.isFullyQualified()) {
          failures.put(ValidationFailure.PRODUCER_OUTPUT_COLUMN_DOES_NOT_MATCH,
              new IllegalArgumentException(String.format("Producer output column and attachment "
              + "column qualifications do not agree.  Both must be either unqualified or fully "
              + "qualified.  Producer column: %s Attachment column: %s",
              producer.getOutputColumn(), columnName)));
        }
        // if outputColumn is null, we cannot validate.
      }

      // validate that data request can be fulfilled by the table.
      KijiDataRequest producerRequest = null;
      try {
        producerRequest = producer.getDataRequest();
      } catch (NullPointerException npe) {
        LOG.debug("NullPointerException thrown by producer.getDataRequest().  Cannot perform "
            + "validation.  Please ensure that your producer is designed to read from the correct "
            + "table.");
      }
      if (producerRequest != null) {
        final KijiTable table = mKiji.openTable(tableName);
        final KijiTableLayout layout;
        try {
          layout = table.getLayout();
        } finally {
          table.release();
        }
        for (Column column : producerRequest.getColumns()) {
          final KijiColumnName name = new KijiColumnName(column.getFamily(), column.getQualifier());
          if (!layout.exists(name)) {
            LOG.debug("Column: {} in producer data request does not exist in table: {}",
                name.toString(), tableName);
            failures.put(ValidationFailure.PRODUCER_REQUEST_CANNOT_BE_FULFILLED,
                new IllegalArgumentException(String.format("Column: %s in producer data request "
                    + "does not exist in table: %s check KijiFreshnessManager log files for other "
                    + "invalid columns.", name.toString(), tableName)));
          }
        }
        // The data request may be null if it is configured using KVStores.  If it is null, we
        // cannot validate.
      }
    } catch (ClassNotFoundException e) {
      LOG.debug("Producer class not found.  Without the producer class, cannot validate that the "
          + "producer output column is of the same type as the attached column nor that the "
          + "producer data request can be fulfilled by the table.");
    }

    failures.putAll(validateAttachment(
        tableName, columnName, record.getProducerClass(), record.getFreshnessPolicyClass(), false));
    return failures;
  }

  /**
   * Validate that all freshness policies attached to columns in the given table conform to the
   * restrictions imposed on attachment.
   *
   * @param tableName the name of the table to validate freshness policies from.
   * @return a map from column name attachment points to a map from {@link ValidationFailure} types
   * to validation exception.  This map will be populated with items only for failed validation
   * checks.  An empty map indicates no validation failures.
   * @throws IOException in case of an error reading from the metatable.
   */
  public Map<KijiColumnName, Map<ValidationFailure, Exception>> validatePolicies(String tableName)
      throws IOException {
    // Check that the table exists.
    if (!mMetaTable.tableExists(tableName)) {
      throw new KijiTableNotFoundException("Couldn't find table: " + tableName);
    }

    final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures =
        new HashMap<KijiColumnName, Map<ValidationFailure, Exception>>();
    final Set<String> keySet = mMetaTable.keySet(tableName);

    for (String key : keySet) {
      if (key.startsWith(METATABLE_KEY_PREFIX)) {
        final Map<ValidationFailure, Exception> innerFailures =
            validatePolicy(tableName, fromMetaTableKey(key).toString());
        if (!innerFailures.isEmpty()) {
          failures.put(fromMetaTableKey(key), innerFailures);
        }
      }
    }
    return failures;
  }

  /**
   * Validates that a set of inputs correctly interoperate as a freshness policy.
   *
   * @param tableName name of the table to validate against.
   * @param columnName column name to validate.  May be a fully qualified column or a family name.
   * @param producerClass KijiProducer class name to validate.
   * @param policyClass KijiFreshnessPolicy class name to validate.
   * @param includeAttachmentOnlyChecks whether the checks are for attachment time (true) or taken
   *     from an existing attached freshness policy (false).
   * @return a map from {@link ValidationFailure} mode to the Exception thrown by that validation
   *     failure.
   * @throws IOException in case of an error reading from the metatable.
   */
  private Map<ValidationFailure, Exception> validateAttachment(
      String tableName,
      String columnName,
      String producerClass,
      String policyClass,
      boolean includeAttachmentOnlyChecks)
      throws IOException {
    // This code will throw an invalid name if there's something wrong with this columnName string.
    final KijiColumnName kcn = new KijiColumnName(columnName);

    final Map<ValidationFailure, Exception> failures = new HashMap<ValidationFailure, Exception>();

    // Check that the policy class name is a valid Java identifier.
    if (!isValidClassName(policyClass)) {
       failures.put(ValidationFailure.BAD_POLICY_NAME, new IllegalArgumentException(String.format(
          "Policy class name: %s is not a valid Java class identifier.", policyClass)));
    }
    // Check that the producer class name is a valid Java identifier.
    if (!isValidClassName(producerClass)) {
      failures.put(ValidationFailure.BAD_PRODUCER_NAME, new IllegalArgumentException(String.format(
          "Producer class name: %s is not a valid Java class identifier.", producerClass)));
    }
    // Check that the table includes the specified column or family.
    final KijiTable table = mKiji.openTable(tableName);
    final Set<KijiColumnName> columnNames;
    final Map<String, FamilyLayout> familyMap;
    try {
      columnNames = table.getLayout().getColumnNames();
      familyMap = table.getLayout().getFamilyMap();
    } finally {
      table.release();
    }
    final Set<String> metadataKeySet = mMetaTable.keySet(tableName);

    // Check that the family exists in the table layout.
    final boolean familyFound;
    if (!familyMap.containsKey(kcn.getFamily())) {
      familyFound = false;
      failures.put(ValidationFailure.NO_FAMILY_IN_TABLE, new IllegalArgumentException(String.format(
          "Table: %s does not contain family: %s", tableName, kcn.getFamily())));
    } else {
      familyFound = true;
    }

    // Check that the column exists if it is fully qualified
    if (kcn.isFullyQualified()) {
      if (familyMap.get(kcn.getFamily()).isGroupType()) {
        // Check that the fully qualified group family column exists in the table layout.
        if (!columnNames.contains(kcn)) {
          failures.put(ValidationFailure.NO_QUALIFIED_COLUMN_IN_TABLE, new IllegalArgumentException(
              String.format("Table: %s does not contain specified column: %s",
              tableName, kcn.toString())));
        }
      }
      if (includeAttachmentOnlyChecks) {
        // Check for a policy attached to kcn and kcn.getFamily();
        if (metadataKeySet.contains(getMetaTableKey(kcn.getFamily()))) {
          failures.put(ValidationFailure.FRESHENER_ALREADY_ATTACHED, new IllegalArgumentException(
              String.format("There is already a freshness policy "
                  + "attached to family: %s Freshness policies may not be attached to a map type "
                  + "family and fully qualified columns within that family.", kcn.getFamily())));
        } else if (metadataKeySet.contains(getMetaTableKey(kcn.getName()))) {
          failures.put(ValidationFailure.FRESHENER_ALREADY_ATTACHED, new IllegalArgumentException(
              String.format("There is already a freshness policy "
                  + "attached to column: %s", kcn.getName())));
        }
      }
    } else {
      // If not fully qualified, check that the family is a map type family.
      if (familyFound) {
        if (!familyMap.get(kcn.toString()).isMapType()) {
          failures.put(ValidationFailure.GROUP_TYPE_FAMILY_ATTACHMENT, new IllegalArgumentException(
              String.format("Specified family: %s is not a valid Map Type family in the table: %s",
                  kcn.toString(), tableName)));
        } else {
          // check for a policy attached to any qualified column in kcn
          boolean qualifiedColumnExists = false;
          for (String key : metadataKeySet) {
            final boolean keyDisqualifies = key.startsWith(getMetaTableKey(kcn.toString()));
            if (keyDisqualifies) {
              LOG.error("Cannot attach freshness policy to family: {} qualified column: {} already "
                  + "has an attached freshness policy.", kcn.toString(),
                  key.substring(METATABLE_KEY_PREFIX.length()));
            }
            qualifiedColumnExists = keyDisqualifies || qualifiedColumnExists;
          }
          if (qualifiedColumnExists) {
            failures.put(ValidationFailure.FRESHENER_ALREADY_ATTACHED, new IllegalArgumentException(
                String.format("There is already a freshness policy "
                    + "attached to a fully qualified column in family: %s Freshness policies may "
                    + "not be attached to a map type family and fully qualified columns within that"
                    + " family. To view a list of attached freshness policies check log files for "
                    + "KijiFreshnessManager.",
                    kcn.toString())));
          }
        }
      }
    }

    return failures;
  }

  /**
   * Closes the manager, freeing resources.
   *
   * @throws IOException if an error occurs.
   */
  public void close() throws IOException {
    mKiji.release();
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
   * Helper method that constructs a meta table key for a column name.
   *
   * @param column the column for which to get a MetaTable key.
   * @return the MetaTable key for the given column.
   */
  private String getMetaTableKey(KijiColumnName column) {
    return METATABLE_KEY_PREFIX + column.getName();
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

  /**
   * Write an already validated KijiFreshnessPolicyRecord to the metatable without further checks.
   *
   * @param tableName the table in which the column lives.
   * @param columnName the column to which the record is attached.
   * @param record the record to attach to the column.
   * @throws IOException in case of an error writing to the metatable.
   */
  private void writeRecordToMetaTable(
      final String tableName,
      final String columnName,
      final KijiFreshnessPolicyRecord record
  ) throws IOException {
    mOutputStream.reset();
    Encoder encoder = mEncoderFactory.directBinaryEncoder(mOutputStream, null);
    mRecordWriter.write(record, encoder);
    mMetaTable.putValue(tableName, getMetaTableKey(columnName),
        mOutputStream.toByteArray());
  }
}
