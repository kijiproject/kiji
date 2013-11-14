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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.impl.InternalFreshKijiTableReader;
import org.kiji.scoring.impl.InternalFreshenerContext;
import org.kiji.scoring.impl.ScoringUtils;

/**
 * This class is responsible for registering, retrieving removing, and validating Freshener records.
 *
 * <p>
 *   Instances of KijiFreshnessManager are thread-safe. Since this class maintains a connection to
 *   the Kiji instance, clients should call {@link #close()} when finished with this class.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class KijiFreshnessManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiFreshnessManager.class);

  public static final Map<String, String> EMPTY_PARAMS = Collections.emptyMap();

  /** Enumeration of possible Validation failure causes. */
  public static enum ValidationFailure {
    ATTACHMENT_COLUMN_NOT_QUALIFIED,
    BAD_POLICY_NAME,
    BAD_SCORE_FUNCTION_NAME,
    FRESHENER_ALREADY_ATTACHED,
    NO_COLUMN_IN_TABLE
  }
  /** The prefix we use for freshness policies stored in a meta table. */
  private static final String METATABLE_KEY_PREFIX = "kiji.scoring.fresh.";
  /** Minimum and maximum KijiFreshenerRecord versions supported by this KijiFreshnessManager. */
  private static final ProtocolVersion MIN_FRESHENER_RECORD_VER =
      ProtocolVersion.parse("freshenerrecord-0.1");
  private static final ProtocolVersion MAX_FRESHENER_RECORD_VER =
      ProtocolVersion.parse("freshenerrecord-0.1");
  /**
   * The freshener version that will be installed by this version of the KijiFreshnessManager.
   * This value is public for testing purposes.
   */
  public static final ProtocolVersion CUR_FRESHENER_RECORD_VER = MAX_FRESHENER_RECORD_VER;
  /** Encoder and Decoder factory for Avro Encoders and Decoders. */
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

  // -----------------------------------------------------------------------------------------------
  // Inner classes
  // -----------------------------------------------------------------------------------------------

  /**
   * An aggregate exception representing one or several validation failures for a single request.
   */
  public static final class FreshenerValidationException extends RuntimeException {
    /** A map of validation failures represented collectively by this excpetion. */
    private final Map<ValidationFailure, Exception> mFailures;

    /**
     * Construct a new FreshenerValidationException from a map of validation failures.
     *
     * @param failures a map of validation failures with which to build this exception.
     */
    public FreshenerValidationException(
        final Map<ValidationFailure, Exception> failures
    ) {
      Preconditions.checkNotNull(failures, "Cannot build a FreshenerValidationException from a null"
          + " failure map.");
      Preconditions.checkArgument(!failures.isEmpty(), "Cannot build a FreshenerValidationException"
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
  public static final class MultiFreshenerValidationException extends RuntimeException {

    private final Map<KijiColumnName, Map<ValidationFailure, Exception>> mFailures;

    /**
     * Construct a new  MultiFreshenerValidationException from a map of validation failures.
     *
     * @param failures a map of validation failure with which to build this exception.
     */
    public MultiFreshenerValidationException(
        final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures
    ) {
      Preconditions.checkNotNull(failures, "Cannot build a MultiFreshenerValidationException from a"
          + " null failure map.");
      Preconditions.checkArgument(!failures.isEmpty(), "Cannot build a "
          + "MultiFreshenerValidationException from an empty failure map.");
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

  // -----------------------------------------------------------------------------------------------
  // Static methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Check if a meta table key is a valid KijiFreshnessManager meta table key.
   *
   * @param metaTableKey the meta table key to check.
   * @return whether the given key is a valid KijiFreshnessManager meta table key.
   */
  private static boolean isKFMMetaTableKey(
      final String metaTableKey
  ) {
    return metaTableKey.startsWith(METATABLE_KEY_PREFIX)
        && metaTableKey.substring(METATABLE_KEY_PREFIX.length()).contains(":");
  }

  /**
   * Helper method that construct a meta table key from a KijiColumnName.
   *
   * @param columnName name of the column from which to create a meta table key.
   * @return a meta table key corresponding to the given column name.
   */
  private static String toMetaTableKey(
      final KijiColumnName columnName
  ) {
    return METATABLE_KEY_PREFIX + columnName.getName();
  }

  /**
   * Helper method that constructs a KijiColumnName from a meta table key.
   *
   * @param metaTableKey the meta table key from which to get a column name.
   * @return the KijiColumnName from the given key.
   */
  private static KijiColumnName fromMetaTableKey(
      final String metaTableKey
  ) {
    return new KijiColumnName(metaTableKey.substring(METATABLE_KEY_PREFIX.length()));
  }

  /**
   * Build a KijiFreshenerRecord from field strings.
   *
   * @param policyClass fully qualified class name of the KijiFreshnessPolicy.
   * @param scoreFunctionClass fully qualified class name of the ScoreFunction.
   * @param parameters configuration parameters.
   * @return a new KijiFreshenerRecord built from the given strings.
   */
  private static KijiFreshenerRecord recordFromStrings(
      final String policyClass,
      final String scoreFunctionClass,
      final Map<String, String> parameters
  ) {
    Preconditions.checkNotNull(policyClass, "KijiFreshnessPolicy class may not be null.");
    Preconditions.checkNotNull(scoreFunctionClass, "ScoreFunction class may not be null.");
    Preconditions.checkNotNull(parameters, "Parameters may not be null.");
    return KijiFreshenerRecord.newBuilder()
        .setRecordVersion(CUR_FRESHENER_RECORD_VER.toCanonicalString())
        .setFreshnessPolicyClass(policyClass)
        .setScoreFunctionClass(scoreFunctionClass)
        .setParameters(parameters)
        .build();
  }

  /**
   * Checks if a String is a valid Java class identifier.
   *
   * <p>This method is package private for testing purposes only.</p>
   *
   * @param className the identifier to check.
   * @return whether the string is a valid Java class identifier.
   */
  static boolean isValidClassName(
      final String className
  ) {
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

  // -----------------------------------------------------------------------------------------------
  // State
  // -----------------------------------------------------------------------------------------------

  /** The Kiji instance within which to manage Fresheners. */
  private final Kiji mKiji;

  /**
   * Initialize a new KijiFreshnessManager for the given Kiji instance.
   *
   * @param kiji a Kiji instance containing the meta table storing freshness information.
   * @throws IOException if there is an error retrieving the meta table.
   */
  private KijiFreshnessManager(Kiji kiji) throws IOException {
    mKiji = kiji.retain();
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

  // -----------------------------------------------------------------------------------------------
  // Private methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Write an already validated KijiFreshenerRecord to the meta table without further checks.
   *
   * @param tableName the table in which the column lives.
   * @param columnName the column to which the record is attached.
   * @param record the record to attach to the column.
   * @throws IOException in case of an error writing to the meta table.
   */
  private void writeRecordToMetaTable(
      final String tableName,
      final KijiColumnName columnName,
      final KijiFreshenerRecord record
  ) throws IOException {
    Preconditions.checkNotNull(
        record.getFreshnessPolicyClass(), "KijiFreshnessPolicy may not be null.");
    Preconditions.checkNotNull(
        record.getScoreFunctionClass(), "ScoreFunction class may not be null.");
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder encoder = ENCODER_FACTORY.directBinaryEncoder(baos, null);
    new SpecificDatumWriter<KijiFreshenerRecord>(KijiFreshenerRecord.SCHEMA$)
        .write(record, encoder);
    mKiji.getMetaTable().putValue(tableName, toMetaTableKey(columnName),
        baos.toByteArray());
  }

  /**
   * Validate a Freshener attachment from strings. Currently this validates that the column is
   * fully qualified and exists in the table and that both the KijiFreshnessPolicy and ScoreFunction
   * class names are valid Java identifiers.
   *
   * @param tableName name of the table which holds the column to check.
   * @param columnName name of the column to which a Freshener attachment is being validated.
   * @param policyClass fully qualified class name of the KijiFreshnessPolicy class to validate.
   * @param scoreFunctionClass fully qualified class name of the ScoreFunction class to validate.
   * @param parameters configuration parameters. Parameters are not currently validated.
   * @return a mapping from ValidationFailure to the Exception which caused that failure.
   * @throws IOException in case of an error reading from the meta table.
   */
  private Map<ValidationFailure, Exception> validateWithStrings(
      final String tableName,
      final KijiColumnName columnName,
      final String policyClass,
      final String scoreFunctionClass,
      final Map<String, String> parameters
  ) throws IOException {
    if (!mKiji.getMetaTable().tableExists(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build());
    }

    final Map<ValidationFailure, Exception> failures = Maps.newHashMap();

    if (!columnName.isFullyQualified()) {
      failures.put(ValidationFailure.ATTACHMENT_COLUMN_NOT_QUALIFIED,
          new IllegalArgumentException(String.format(
              "Attachment column must be fully qualified. Found: %s", columnName)));
    }

    if (!isValidClassName(policyClass)) {
      failures.put(ValidationFailure.BAD_POLICY_NAME, new IllegalArgumentException(String.format(
          "KijiFreshnessPolicy class name is not a valid Java identifier. Found: %s",
          policyClass)));
    }

    if (!isValidClassName(scoreFunctionClass)) {
      failures.put(ValidationFailure.BAD_SCORE_FUNCTION_NAME, new IllegalArgumentException(
          String.format("ScoreFunction class name is not a valid Java identifier. Found: %s",
          scoreFunctionClass)));
    }

    final KijiTable table = mKiji.openTable(tableName);
    try {
      final Map<String, FamilyLayout> familyMap = table.getLayout().getFamilyMap();
      if (familyMap.containsKey(columnName.getFamily())) {
        if (familyMap.get(columnName.getFamily()).isGroupType()) {
          if (!table.getLayout().getColumnNames().contains(columnName)) {
            failures.put(ValidationFailure.NO_COLUMN_IN_TABLE, new IllegalArgumentException(
                String.format("Column: %s not found in table: %s", columnName, table.getURI())));
          }
        }
      } else {
        failures.put(ValidationFailure.NO_COLUMN_IN_TABLE, new IllegalArgumentException(
            String.format("Family of column: %s not found in table: %s",
            columnName, table.getURI())));
      }


    } finally {
      table.release();
    }

    return failures;
  }

  /**
   * Validate that a column can legally have a new Freshener attached. Currently this only checks
   * that there is not already another Freshener attached to the given column.
   *
   * @param tableName name of the table which holds the column to check.
   * @param columnName name of the column to check.
   * @return a mapping from ValidationFailure to the Exception which caused that failure.
   * @throws IOException in case of an error reading from the meta table.
   */
  private Map<ValidationFailure, Exception> validateAttachment(
      final String tableName,
      final KijiColumnName columnName
  ) throws IOException {
    if (!mKiji.getMetaTable().tableExists(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build());
    }

    final Map<ValidationFailure, Exception> failures = Maps.newHashMap();

    final Set<String> keySet = mKiji.getMetaTable().keySet(tableName);
    if (keySet.contains(toMetaTableKey(columnName))) {
      failures.put(ValidationFailure.FRESHENER_ALREADY_ATTACHED, new IllegalArgumentException(
          String.format("A Freshener is already attached to column: %s", columnName)));
    }

    return failures;
  }

  /**
   * Validate that a KijiFreshenerRecord may be written to the meta table. Aggregates validation
   * from {@link #validateWithStrings(String, org.kiji.schema.KijiColumnName, String, String,
   * java.util.Map)} and {@link #validateAttachment(String, org.kiji.schema.KijiColumnName)} (if
   * overwriteExisting is false).
   *
   * @param tableName name of the table which holds the column to which the Freshener to validate
   *     is or will be attached.
   * @param columnName name of the column to which the Freshener to validate is or will be attached.
   * @param record KijiFreshenerRecord to validate.
   * @param overwriteExisting whether to check for a Freshener attached to the given column. Checks
   *     when false.
   * @return a map from validation failure type to the Exception which represents that failure.
   * @throws IOException in case of an error reading from the meta table.
   */
  private Map<ValidationFailure, Exception> validateRecord(
      final String tableName,
      final KijiColumnName columnName,
      final KijiFreshenerRecord record,
      final boolean overwriteExisting
  ) throws IOException {
    final Map<ValidationFailure, Exception> failures = Maps.newHashMap();
    if (null == record) {
      return failures;
    }
    Preconditions.checkNotNull(
        record.getFreshnessPolicyClass(), "KijiFreshnessPolicy may not be null.");
    Preconditions.checkNotNull(
        record.getScoreFunctionClass(), "ScoreFunction may not be null.");

    if (!columnName.isFullyQualified()) {
      failures.put(ValidationFailure.ATTACHMENT_COLUMN_NOT_QUALIFIED, new IllegalArgumentException(
          String.format("Attachment column: '%s' is not fully qualified. Fresheners may only be "
          + "attached to fully qualified columns.", columnName)));
    }

    failures.putAll(validateWithStrings(
        tableName,
        columnName,
        record.getFreshnessPolicyClass(),
        record.getScoreFunctionClass(),
        record.getParameters()));

    if (!overwriteExisting) {
      failures.putAll(validateAttachment(tableName, columnName));
    }

    return failures;
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface
  // -----------------------------------------------------------------------------------------------

  /**
   * Register a Freshener record to the specified column in the specified table. The record will be
   * built from policy, scoreFunction, and parameters.
   *
   * @param tableName the name of the table which holds the specified column.
   * @param columnName the name column to which to attach a Freshener.
   * @param policy an already initialized instance of the KijiFreshnessPolicy class to use in the
   *     Freshener.
   * @param scoreFunction an already initialized instance of the ScoreFunction class to use in the
   *     Freshener.
   * @param parameters configuration parameters which will be available to the KijiFreshnessPolicy
   *     and ScoreFunction via their context objects.
   * @param overwriteExisting whether to overwrite an existing KijiFreshenerRecord in the target
   *     column.
   * @param setupClasses whether to setup the KijiFreshnessPolicy and ScoreFunction classes before
   *     calling their serializeToParameters() methods. This will also call getRequiredStores on
   *     both objects and may open the KeyValueStores they return.
   * @throws IOException in case of an error writing to the meta table.
   */
  public void registerFreshener(
      final String tableName,
      final KijiColumnName columnName,
      final KijiFreshnessPolicy policy,
      final ScoreFunction scoreFunction,
      final Map<String, String> parameters,
      final boolean overwriteExisting,
      final boolean setupClasses
  ) throws IOException {
    if (setupClasses) {
      final InternalFreshenerContext context =
          InternalFreshenerContext.create(columnName, parameters);
      final KeyValueStoreReaderFactory factory =
          InternalFreshKijiTableReader.createKVStoreReaderFactory(context, scoreFunction, policy);
      context.setKeyValueStoreReaderFactory(factory);
      policy.setup(context);
      scoreFunction.setup(context);
    }
    final Map<String, String> combinedParameters = Maps.newHashMap();
    combinedParameters.putAll(scoreFunction.serializeToParameters());
    combinedParameters.putAll(policy.serializeToParameters());
    combinedParameters.putAll(parameters);

    final KijiFreshenerRecord record = recordFromStrings(
        policy.getClass().getName(), scoreFunction.getClass().getName(), combinedParameters);

    final Map<ValidationFailure, Exception> validationFailures =
        validateRecord(tableName, columnName, record, overwriteExisting);

    if (validationFailures.isEmpty()) {
      writeRecordToMetaTable(tableName, columnName, record);
    } else {
      throw new FreshenerValidationException(validationFailures);
    }
  }

  /**
   * Register a Freshener record to the specified column in the specified table. The record will be
   * built from policyClass, policyState, scoreFunctionClass, scoreFunctionState, and
   * parameters.
   *
   * @param tableName the name of the table which holds the specified column.
   * @param columnName the name column to which to attach a Freshener.
   * @param policyClass fully qualified class name of the KijiFreshnessPolicy class to use in the
   *     Freshener.
   * @param scoreFunctionClass fully qualified class name of the ScoreFunction class to use in the
   *     Freshener.
   * @param parameters configuration parameters which will be available to the KijiFreshnessPolicy
   *     and ScoreFunction via their context objects.
   * @param overwriteExisting whether to overwrite an existing KijiFreshenerRecord in the target
   *     column.
   * @param instantiateClasses whether to instantiate classes in order to call
   *     serializeToParameters(). (This requires classes on the classpath).
   * @param setupClasses whether to setup the KijiFreshnessPolicy and ScoreFunction classes before
   *     calling their serializeToParameters() methods. This will also call getRequiredStores on
   *     both objects and may open the KeyValueStores they return. This option has no effect if
   *     instantiateClasses is false.
   * @throws IOException in case of an error writing to the meta table.
   */
  // CSOFF: ParameterNumber
  public void registerFreshener(
      final String tableName,
      final KijiColumnName columnName,
      final String policyClass,
      final String scoreFunctionClass,
      final Map<String, String> parameters,
      final boolean overwriteExisting,
      final boolean instantiateClasses,
      final boolean setupClasses
  ) throws IOException {
    // CSON: ParameterNumber
    if (instantiateClasses) {
      final KijiFreshnessPolicy policy = ScoringUtils.policyForName(policyClass);
      final ScoreFunction scoreFunction = ScoringUtils.scoreFunctionForName(scoreFunctionClass);

      registerFreshener(
          tableName,
          columnName,
          policy,
          scoreFunction,
          parameters,
          overwriteExisting,
          setupClasses);
    } else {
      final KijiFreshenerRecord record =
          recordFromStrings(policyClass, scoreFunctionClass, parameters);
      final Map<ValidationFailure, Exception> validationFailures =
          validateRecord(tableName, columnName, record, overwriteExisting);

      if (validationFailures.isEmpty()) {
        writeRecordToMetaTable(tableName, columnName, record);
      } else {
        throw new FreshenerValidationException(validationFailures);
      }
    }
  }

  /**
   * Register a Freshener record to the specified column in the specified table.
   *
   * @param tableName name of the table which holds the column to which to attach the Freshener.
   * @param columnName name of the column to which to attach the Freshener.
   * @param record KijiFreshenerRecord representing the Freshener to attach.
   * @param overwriteExisting whether to overwrite an existing Freshener attached to the specified
   *     column.
   * @param instantiateClasses whether to instantiate classes in order to call
   *     serializeToParameters(). (This requires classes on the classpath).
   * @param setupClasses whether to setup the KijiFreshnessPolicy and ScoreFunction classes before
   *     calling their serializeToParameters() methods. This will also call getRequiredStores on
   *     both objects and may open the KeyValueStores they return. This option has no effect if
   *     instantiateClasses is false.
   * @throws IOException in case of an error writing to the meta table.
   */
  public void registerFreshener(
      final String tableName,
      final KijiColumnName columnName,
      final KijiFreshenerRecord record,
      final boolean overwriteExisting,
      final boolean instantiateClasses,
      final boolean setupClasses
  ) throws IOException {
    if (instantiateClasses) {
      final KijiFreshnessPolicy policy =
          ScoringUtils.policyForName(record.getFreshnessPolicyClass());
      final ScoreFunction scoreFunction =
          ScoringUtils.scoreFunctionForName(record.getScoreFunctionClass());

      registerFreshener(
          tableName,
          columnName,
          policy,
          scoreFunction,
          record.getParameters(),
          overwriteExisting,
          setupClasses);
    } else {
      final Map<ValidationFailure, Exception> validationFailures = validateRecord(
          tableName,
          columnName,
          record,
          overwriteExisting);

      if (validationFailures.isEmpty()) {
        writeRecordToMetaTable(tableName, columnName, record);
      } else {
        throw new FreshenerValidationException(validationFailures);
      }
    }
  }

  /**
   * Register a collection of Freshener records to columns in the specified table.
   *
   * @param tableName the name of the table which holds the specified columns.
   * @param records a map from names of columns in the specified table to Freshener records to
   *     register to those columns.
   * @param overwriteExisting whether to overwrite existing Freshener records or throw a validation
   *     exception if there is a Freshener attached to a specified column.
   * @param instantiateClasses whether to instantiate classes to enable validation which requires
   *     class objects. This validation is more thorough, but requires classes to be available on
   *     the classpath.
   * @param setupClasses whether to setup the KijiFreshnessPolicy and ScoreFunction classes before
   *     calling their serializeToParameters() methods. This will also call getRequiredStores on
   *     both objects and may open the KeyValueStores they return. This option has no effect if
   *     instantiateClasses is false.
   * @throws IOException in case of an error reading from or writing to the meta table.
   */
  public void registerFresheners(
      final String tableName,
      final Map<KijiColumnName, KijiFreshenerRecord> records,
      final boolean overwriteExisting,
      final boolean instantiateClasses,
      final boolean setupClasses
  ) throws IOException {
    final Map<KijiColumnName, Map<ValidationFailure, Exception>> combinedFailures =
        Maps.newHashMap();

    for (Map.Entry<KijiColumnName, KijiFreshenerRecord> recordEntry : records.entrySet()) {
      if (instantiateClasses) {
        final KijiFreshnessPolicy policy =
            ScoringUtils.policyForName(recordEntry.getValue().getFreshnessPolicyClass());
        final ScoreFunction scoreFunction =
            ScoringUtils.scoreFunctionForName(recordEntry.getValue().getScoreFunctionClass());

        registerFreshener(
            tableName,
            recordEntry.getKey(),
            policy,
            scoreFunction,
            recordEntry.getValue().getParameters(),
            overwriteExisting,
            setupClasses);
      } else {
        final Map<ValidationFailure, Exception> individualFailures = validateRecord(
          tableName,
          recordEntry.getKey(),
          recordEntry.getValue(),
          overwriteExisting);

        if (!individualFailures.isEmpty()) {
          combinedFailures.put(recordEntry.getKey(), individualFailures);
        }
      }
    }
    if (combinedFailures.isEmpty()) {
      for (Entry<KijiColumnName, KijiFreshenerRecord> recordEntry : records.entrySet()) {
        writeRecordToMetaTable(tableName, recordEntry.getKey(), recordEntry.getValue());
      }
    } else {
      throw new MultiFreshenerValidationException(combinedFailures);
    }
  }

  /**
   * Retrieve the Freshener record for the specified column in the specified table.
   *
   * @param tableName the name of the table which holds the column for which to retrieve a Freshener
   *     record.
   * @param columnName the name of the column for which to retrieve a Freshener record.
   * @return the KijiFreshenerRecord attached to the given column in the given table.
   * @throws IOException in case of an error reading from the meta table.
   */
  public KijiFreshenerRecord retrieveFreshenerRecord(
      final String tableName,
      final KijiColumnName columnName
  ) throws IOException {
    final String metaTableKey = toMetaTableKey(columnName);

    final byte[] recordBytes;
    try {
      recordBytes = mKiji.getMetaTable().getValue(tableName, metaTableKey);
    } catch (IOException ioe) {
      if (ioe.getMessage().equals(String.format(
          "Could not find any values associated with table %s and key %s",
          tableName, metaTableKey))) {
        return null;
      } else {
        throw ioe;
      }
    }
    final Decoder decoder = DECODER_FACTORY.binaryDecoder(recordBytes, null);
    return new SpecificDatumReader<KijiFreshenerRecord>(KijiFreshenerRecord.SCHEMA$)
        .read(null, decoder);
  }

  /**
   * Retrieve the Freshener records for all columns in the specified table.
   *
   * @param tableName the name of the table for which to retrieve all attached Fresheners.
   * @return a map from column name to attached Freshener record.
   * @throws IOException in case of an error reading from the meta table.
   */
  public Map<KijiColumnName, KijiFreshenerRecord> retrieveFreshenerRecords(
      final String tableName
  ) throws IOException {
    final Set<String> keySet = mKiji.getMetaTable().keySet(tableName);
    final Map<KijiColumnName, KijiFreshenerRecord> records = Maps.newHashMap();

    for (String key : keySet) {
      if (isKFMMetaTableKey(key)) {
        final KijiColumnName column = fromMetaTableKey(key);
        final KijiFreshenerRecord record = retrieveFreshenerRecord(tableName, column);
        if (null != record) {
          records.put(column, record);
        }
      }
    }

    return records;
  }

  /**
   * Remove a Freshener attached to the specified column in the specified table.
   *
   * @param tableName the name of the table which holds the column from which to remove a Freshener.
   * @param columnName the name of the column from which to remove a Freshener.
   * @throws IOException in case of an error writing to the meta table.
   */
  public void removeFreshener(
      final String tableName,
      final KijiColumnName columnName
  ) throws IOException {
    if (!mKiji.getMetaTable().tableExists(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build());
    } else {
      final String metaTableKey = toMetaTableKey(columnName);
      Preconditions.checkArgument(mKiji.getMetaTable().keySet(tableName).contains(metaTableKey),
          "There is no Freshener attached to column: %s in table: %s", columnName, tableName);
      mKiji.getMetaTable().removeValues(tableName, metaTableKey);
    }
  }

  /**
   * Remove the Fresheners attached to all columns in the specified table.
   *
   * @param tableName the name of the table from which to remove all attached Fresheners.
   * @return the set of columns from which Fresheners were removed.
   * @throws IOException in case of an error reading from or writing to the meta table.
   */
  public Set<KijiColumnName> removeFresheners(
      final String tableName
  ) throws IOException {
    if (!mKiji.getMetaTable().tableExists(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build());
    } else {
      final Set<String> keySet = mKiji.getMetaTable().keySet(tableName);
      final Set<KijiColumnName> removedColumns = Sets.newHashSet();
      for (String key : keySet) {
        if (isKFMMetaTableKey(key)) {
          mKiji.getMetaTable().removeValues(tableName, key);
          removedColumns.add(fromMetaTableKey(key));
        }
      }
      return removedColumns;
    }
  }

  /**
   * Validate that the Freshener attached to the specified column in the specified table conforms to
   * the restrictions imposed on Freshener attachment.
   *
   * @param tableName the name of the table which holds the column whose attachment to validate.
   * @param columnName the name of the column whose attachment to validate.
   * @return a map from {@link ValidationFailure} to
   *     {@link org.kiji.scoring.KijiFreshnessManager.FreshenerValidationException}. This map will
   *     be populated with items for failed validation checks only. An empty map indicates no
   *     validation failures.
   * @throws IOException in case of an error reading from the meta table.
   */
  public Map<ValidationFailure, Exception> validateFreshener(
      final String tableName,
      final KijiColumnName columnName
  ) throws IOException {
    if (!mKiji.getMetaTable().tableExists(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build());
    } else {
      final KijiFreshenerRecord record = retrieveFreshenerRecord(tableName, columnName);

      return validateRecord(tableName, columnName, record, false);
    }
  }

  /**
   * Validate that all Fresheners attached to columns in the specified table conform to the
   * restrictions imposed on Freshener attachment.
   *
   * @param tableName the name of the table for which to validate Freshener attachments.
   * @return a map from column name attachment points to a map from {@link ValidationFailure} types
   *     to {@link org.kiji.scoring.KijiFreshnessManager.FreshenerValidationException}. This map
   *     will be populated with items for failed validation checks only. An empty map indicates no
   *     validation failures.
   * @throws IOException in case of an error reading from the meta table.
   */
  public Map<KijiColumnName, Map<ValidationFailure, Exception>> validateFresheners(
      final String tableName
  ) throws IOException {
    if (!mKiji.getMetaTable().tableExists(tableName)) {
      throw new KijiTableNotFoundException(
          KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build());
    } else {
      final Set<String> keySet = mKiji.getMetaTable().keySet(tableName);

      final Map<KijiColumnName, Map<ValidationFailure, Exception>> combinedFailures =
          Maps.newHashMap();

      for (String key : keySet) {
        if (isKFMMetaTableKey(key)) {
          final KijiColumnName column = fromMetaTableKey(key);
          final Map<ValidationFailure, Exception> individualFailures =
              validateFreshener(tableName, column);
          if (!individualFailures.isEmpty()) {
            combinedFailures.put(column, individualFailures);
          }
        }
      }

      return combinedFailures;
    }
  }

  /**
   * Closes the manager, freeing resources.
   *
   * @throws IOException if there is an error releasing resources.
   */
  public void close() throws IOException {
    mKiji.release();
  }
}
