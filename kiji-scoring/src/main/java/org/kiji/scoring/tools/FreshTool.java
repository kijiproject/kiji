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

package org.kiji.scoring.tools;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.Flag;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessManager.FreshenerValidationException;
import org.kiji.scoring.KijiFreshnessManager.ValidationFailure;
import org.kiji.scoring.avro.KijiFreshenerRecord;

/**
 * Command line interface tool for registering, retrieving, removing, and validating Fresheners.
 *
 * <p>
 *   The Kiji Fresh tool performs operations on Kiji tables and columns. Allowed operations are:
 *   <li>register (requires exactly one specified column) to attach a Freshener to a column.</li>
 *   <li>retrieve (accepts a table or column) to print Fresheners attached to columns in the
 *       specified table, or the Freshener attached to the specified column.</li>
 *   <li>remove (accepts a table or column) to remove Fresheners attached to columns in the
 *       specified table, or the Freshener attached to the specified column.</li>
 *   <li>validate (accepts a table or column) to validate Freshener attached to columns in the
 *       specified table, or the Freshener attached to the specified column.</li>
 * </p>
 * <p>
 *   Example usages:
 *   <pre>
 *     kiji fresh --target=kiji://.env/default/table/family:qualifier --do=register \
 *         --policy-class=com.mycompany.Policy \
 *         --score-function-class=com.mycompany.ScoreFunction \
 *         --parameters='{"key1":"value1", "key2":"value2"}
 *     kiji fresh --target=kiji://.env/default/table/family:qualifier --do=retrieve
 *     kiji fresh --target=kiji://.env/default/table/family:qualifier --do=remove
 *     kiji fresh --target=kiji://.env/default/table --do=validate
 *   </pre>
 * </p>
 */
public class FreshTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(FreshTool.class);

  @Flag(name="target", usage="the KijiURI of the target of your operation. Must include at "
      + "least a table and may include a column.")
  private String mTargetFlag = null;

  @Flag(name="do", usage="Pick exactly one of:\n"
      + "          'register' to register a Freshener for the specified column.\n"
      + "            (requires --policy-class, --score-function-class, and exactly one column in "
      + "--target) (optionally --parameters, --instantiate-classes, --overwrite-existing, "
      + "--setup-classes)\n"
      + "          'retrieve' to retrieve the Freshener record(s) for the specified table or "
      + "column.\n"
      + "          'remove' to remove the Freshener record(s) from the specified table or column.\n"
      + "          'validate' to run validation on the Freshener record(s) attached to the "
      + "specified table or column.\n"
      + "            (optionally --instantiate-classes)")
  private String mDoFlag = null;

  @Flag(name="policy-class", usage="fully qualified class name of a KijiFreshnessPolicy "
      + "implementation to include in a Freshener record.")
  private String mPolicyClassFlag = null;

  @Flag(name="score-function-class", usage="fully qualified class name of a ScoreFunction "
      + "implementation to include in a Freshener record.")
  private String mScoreFunctionClassFlag = null;

  @Flag(name="parameters", usage="JSON encoded mapping of configuration parameters to include in a "
      + "Freshener record. Assumed empty if unspecified.")
  private String mParametersFlag = "{}";

  @Flag(name="instantiate-classes", usage="instruct the tool to instantiate classes to call their "
      + "serializeToParameters methods to include in the record. "
      + "(requires classes on the classpath)")
  private Boolean mInstantiateClassesFlag = false;

  @Flag(name="overwrite-existing", usage="instruct the tool to overwrite existing records when "
      + "registering a Freshener.")
  private Boolean mOverwriteExistingFlag = false;

  @Flag(name="setup-classes", usage="instruct the tool to call setup on the KijiFreshnessPolicy and"
      + " ScoreFunction objects created if --instantiate-classe=true (does nothing if "
      + "--instantiate-classes=false. The tool will construct a FreshenerSetupContext using the "
      + "other parameters specified here.")
  private Boolean mSetupClassesFlag = false;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "fresh";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "register, retrieve, remove, or validate Freshener records";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /** GSON used for serializing and deserializing parameter maps and printing records. */
  private static final Gson GSON = new Gson();

  /** Exit code to indicate that the tool ran normally, but detected an invalid Freshener. */
  public static final int VALIDATION_FAILURE = 2;

  /** Operation selector mode. */
  private static enum DoMode {
    REGISTER, RETRIEVE, REMOVE, VALIDATE
  }

  private KijiURI mURI = null;
  private Kiji mKiji = null;
  private KijiFreshnessManager mManager = null;
  private DoMode mDoMode = null;

  /**
   * Parse a string-string map from JSON.
   *
   * @param serializedMap the JSON serialized map.
   * @return the deserialized version of the map.
   */
  private static Map<String, String> mapFromJSON(
      final String serializedMap
  ) {
    return GSON.fromJson(serializedMap, Map.class);
  }

  /**
   * Serialize a map to JSON.
   *
   * @param map the map to serialize.
   * @return the seriaizlied version of the map.
   */
  private static String mapToJSON(
      final Map<String, String> map
  ) {
    return GSON.toJson(map, Map.class);
  }

  /**
   * Pretty print validation failures from a {@link FreshenerValidationException} for the given
   * column.
   *
   * @param column the column to which the invalid Freshener is attached.
   * @param failures return value of {@link FreshenerValidationException#getExceptions()} to pretty
   *     print.
   */
  private static void printFreshenerValidationErrors(
      final KijiColumnName column,
      final Map<ValidationFailure, Exception> failures
  ) {
    final StringBuilder errorMessage =
        new StringBuilder(String.format("There were validation failures in column: %s%n", column));
    for (Map.Entry<ValidationFailure, Exception> failure : failures.entrySet()) {
      errorMessage.append(String.format(
          "  %s: %s%n", failure.getKey(), failure.getValue().getMessage()));
    }
    throw new ToolError(errorMessage.toString());
  }

  /**
   * Pretty print validation failures from a MultiFreshenerValidationException.
   *
   * @param failures return value of a
   *     {@link org.kiji.scoring.KijiFreshnessManager.MultiFreshenerValidationException}'s
   *     getExceptions() method to pretty print.
   */
  private static void printMultiFreshenerValidationErrors(
      final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures
  ) {
    final StringBuilder errorMessage =
        new StringBuilder(String.format("There were validation failures:%n"));
    for (Map.Entry<KijiColumnName, Map<ValidationFailure, Exception>> columnEntry
        : failures.entrySet()) {
      errorMessage.append(String.format("  %s:%n", columnEntry.getKey()));
      for (Map.Entry<ValidationFailure, Exception> failure : columnEntry.getValue().entrySet()) {
        errorMessage.append(String.format(
            "    %s: %s%n", failure.getKey(), failure.getValue().getMessage()));
      }
    }
    throw new ToolError(errorMessage.toString());
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    if (null == mTargetFlag) {
      throw new ToolError(
          "--target is required. Please specify the KijiURI of the target table or column(s).");
    } else {
      try {
        mURI = KijiURI.newBuilder(mTargetFlag).build();
      } catch (KijiURIException kurie) {
        throw new ToolError(kurie.getMessage());
      }
      if (null == mURI.getTable()) {
        throw new ToolError("--target URI must include at least a table.");
      }
      if (1 < mURI.getColumns().size()) {
        throw new ToolError("--target URI may not include more than one column.");
      }
    }
    if (null == mDoFlag) {
      throw new ToolError("--do is required. Please specify one of register, retrieve, remove,"
          + " or validate.");
    } else {
      try {
        mDoMode = DoMode.valueOf(mDoFlag.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException iae) {
        throw new ToolError("Invalid --do command. Valid commands are register, retrieve, "
            + "remove, and validate");
      }
      switch (mDoMode) {
        case REGISTER: {
          if (1 != mURI.getColumns().size()) {
            throw new ToolError("--do=register requires exactly one column in --target.");
          }
          if (null == mPolicyClassFlag || null == mScoreFunctionClassFlag) {
            throw new ToolError("--do=register requires --policy-class, --score-function-class.");
          }
          break;
        }
        case RETRIEVE: break; // Nothing to validate for retrieve.
        case REMOVE: break; // Nothing to validate for remove.
        case VALIDATE: break; // Nothing to validate for validate.
        default: throw new InternalKijiError("Unknown DoMode: " + mDoMode);
      }
    }
  }

  /**
   * Register the specified field flags to all columns specified in mURI (there should only be one
   * column).
   *
   * @throws IOException in case of an error register the Freshener.
   */
  private void registerFlagsToColumns() throws IOException {
    for (KijiColumnName column : mURI.getColumns()) {
      try {
        mManager.registerFreshener(
            mURI.getTable(),
            column,
            mPolicyClassFlag,
            mScoreFunctionClassFlag,
            mapFromJSON(mParametersFlag),
            mOverwriteExistingFlag,
            mInstantiateClassesFlag,
            mSetupClassesFlag);
      } catch (FreshenerValidationException fve) {
        printFreshenerValidationErrors(column, fve.getExceptions());
      }
    }
  }

  /**
   * Print a KijiFreshenerRecord.
   *
   * @param attachedColumn name of the column to which the record is attached.
   * @param record KijiFreshenerRecord to print.
   */
  private void printRecord(
      final KijiColumnName attachedColumn,
      final KijiFreshenerRecord record
  ) {
    final StringBuilder builder = new StringBuilder();
    if (null == record) {
      builder.append(String.format("No Freshener attached to column: '%s' in table: '%s'%n",
          attachedColumn, mURI.getTable()));
    } else {
      builder.append(String.format("Freshener attached to column: '%s' in table: '%s'%n",
          attachedColumn, mURI.getTable()));
      builder.append(
          String.format("  KijiFreshnessPolicy class: '%s'%n", record.getFreshnessPolicyClass()));
      builder.append(
          String.format("  ScoreFunction class: '%s'%n", record.getScoreFunctionClass()));
      builder.append(String.format("  Parameters: %s%n", mapToJSON(record.getParameters())));
    }
    getPrintStream().print(builder.toString());
  }

  /**
   * Perform the requested registration and return the appropriate tool exit code. (Called only if
   * mDoMode == REGISTER)
   *
   * @return the tool exit code based on the outcome of attempted Freshener registration.
   * @throws IOException in case of an error registering a Freshener.
   */
  private int register() throws IOException {
    registerFlagsToColumns();
    return BaseTool.SUCCESS;
  }

  /**
   * Perform the requested retrieval and return the appropriate tool exit code. (Called only if
   * mDoMode == RETRIEVE)
   *
   * @return the tool exit code based on the outcome of attempted Freshener retrieval.
   * @throws IOException in case of an error retrieving a Freshener.
   */
  private int retrieve() throws IOException {
    final Collection<KijiColumnName> columns = mURI.getColumns();
    if (columns.isEmpty()) {
      final Map<KijiColumnName, KijiFreshenerRecord> records =
          mManager.retrieveFreshenerRecords(mURI.getTable());
      if (records.isEmpty()) {
        getPrintStream().printf(
            "No Fresheners attached to columns in table: '%s'%n", mURI.getTable());
      } else {
        for (Map.Entry<KijiColumnName, KijiFreshenerRecord> recordEntry : records.entrySet()) {
          printRecord(recordEntry.getKey(), recordEntry.getValue());
        }
      }
    } else {
      for (KijiColumnName column : columns) {
        printRecord(column, mManager.retrieveFreshenerRecord(mURI.getTable(), column));
      }
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Perform requested remove operations and return the appropriate tool exit code. (Called only if
   * mDoMode == REMOVE)
   *
   * @return the tool exit code based on the outcome of attempted Freshener removal.
   * @throws IOException in case of an error removing a Freshener.
   */
  private int remove() throws IOException {
    final Collection<KijiColumnName> columns = mURI.getColumns();
    if (columns.isEmpty()) {
      final Set<KijiColumnName> removed = mManager.removeFresheners(mURI.getTable());
      getPrintStream().println("Fresheners removed from:");
      for (KijiColumnName column : removed) {
        getPrintStream().println("  " + column);
      }
    } else {
      getPrintStream().println("Fresheners removed from:");
      for (KijiColumnName column : columns) {
        mManager.removeFreshener(mURI.getTable(), column);
        getPrintStream().println("  " + column);
      }
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Perform requested validation and return the appropriate tool exit code. (Called only if
   * mDoMode == VALIDATE)
   *
   * @return the tool exit code based on the outcome of attempted validation.
   * @throws IOException in case of an error performing validation.
   */
  private int validate() throws IOException {
    final Collection<KijiColumnName> columns = mURI.getColumns();
    if (columns.isEmpty()) {
      final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures =
          mManager.validateFresheners(mURI.getTable());
      if (failures.isEmpty()) {
        getPrintStream().printf(
            "All Fresheners attached to columns in table: %s are valid.%n", mURI.getTable());
        return BaseTool.SUCCESS;
      } else {
        printMultiFreshenerValidationErrors(failures);
        return VALIDATION_FAILURE;
      }
    } else {
      boolean validationFailed = false;
      for (KijiColumnName column : columns) {
        final Map<ValidationFailure, Exception> failures =
            mManager.validateFreshener(mURI.getTable(), column);
        if (failures.isEmpty()) {
          getPrintStream().printf("%s is valid.%n", column);
        } else {
          validationFailed = true;
          printFreshenerValidationErrors(column, failures);
        }
      }
      return (validationFailed) ? VALIDATION_FAILURE : BaseTool.SUCCESS;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    mKiji = Kiji.Factory.open(mURI);
    mManager = KijiFreshnessManager.create(mKiji);

    switch (mDoMode) {
      case REGISTER: {
        return register();
      }
      case RETRIEVE: {
        return retrieve();
      }
      case REMOVE: {
        return remove();
      }
      case VALIDATE: {
        return validate();
      }
      default: throw new InternalKijiError("Unknown DoMode: " + mDoMode);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    if (null != mKiji) {
      mKiji.release();
    }
    if (null != mManager) {
      mManager.close();
    }
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new FreshTool(), args));
  }
}
