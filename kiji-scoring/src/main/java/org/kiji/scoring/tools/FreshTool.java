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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiURIException;
import org.kiji.schema.tools.BaseTool;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessManager.FreshnessValidationException;
import org.kiji.scoring.KijiFreshnessManager.ValidationFailure;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * Command line interface tool for registering and inspecting freshness policies.
 *
 * Usage:
 *  <p><pre>
 *  // Print all freshness policies attached to a table
 *  kiji fresh kiji://.env/instance/table --do=retrieve-all
 *  // Print the freshness policy attached to a column.  (If multiple columns are specified, will
 *  // print each freshness policy)
 *  kiji fresh kiji://.env/instance/table/family:qualifier --do=retrieve
 *  // Register a freshness policy for a column
 *  kiji fresh kiji://.env/instance/table/family:qualifer --do=register \
 *    --policy-class=org.kiji.scoring.lib.ShelfLife \
 *    --policy-state={"shelfLife":10} \
 *    --producer-class=com.mycompany.freshening.RecommendingProducer
 *  // Unregister a freshness policy from a column
 *  kiji fresh kiji://.env/instance/table/family:qualifier --do=unregister
 *  // Validate the freshness policy attached to a column
 *  kiji fresh kiji://.env/instance/table/family:qualifier --do=validate
 *  // Validate all freshness policies attached to a table
 *  kiji fresh kiji://.env/instance/table
 *  </pre></p>
 *  <p>If the --as-strings flag (default = false) is not set, the kiji fresh tool will
 *  checks on class names to ensure classes are available on the classpath.  If --interactive
 *  (default = true) is true and classes cannot be found on the classpath, the user will be
 *  prompted to set the --as-strings flag to continue.</p>
 */
public class FreshTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(FreshTool.class);

  // Positional argument for the table or column

  @Flag(name="do", usage=
      "\"register (requires --policy-class and --producer-class, --policy-state will be assumed "
      + "empty unless specified.)\"; "
      + "\"unregister\"; "
      + "\"retrieve\"; "
      + "\"retrieve-all\"; "
      + "\"validate\"; "
      + "\"validate-all\";")
  private String mDoFlag = "";

  @Flag(name="policy-class", usage="fully qualified name of a KijiFreshnessPolicy class.")
  private String mPolicyClassFlag;

  @Flag(name="policy-state", usage="serialized state of the KijiFreshnessPolicy, will be passed "
      + "to KijiFreshnessPolicy.deserialize().  Specify exactly one of policy-state or "
      + "policy-state-file")
  private String mPolicyStateFlag;

  @Flag(name="policy-state-file", usage="serialized state of the KijiFreshnessPolicy, will be "
      + "passed to KijiFreshnessPolicy.deserialize().  Specify exactly one of policy-state or "
      + "policy-state-file")
  private String mPolicyStateFileFlag;

  @Flag(name="producer-class", usage="fully qualified name of a KijiProducer class.")
  private String mProducerClassFlag;

  @Flag(name="as-strings", usage="set to true to write strings directly without checking for "
      + "classes on the classpath.")
  private Boolean mAsStringFlag = false;

  /** URI of the Kiji table or column on which to perform an operation. */
  private KijiURI mURI = null;

  /** Kiji instance housing the metatable for the target KijiTable or column. */
  private Kiji mKiji = null;

  /** KijiFreshnessManager for the target table. */
  private KijiFreshnessManager mManager = null;

  /** Operation selector mode. */
  private static enum DoMode {
    REGISTER, UNREGISTER, RETRIEVE, RETRIEVE_ALL, UNREGISTER_ALL, VALIDATE, VALIDATE_ALL
  }

  /** Operation Mode. */
  private DoMode mDoMode;

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "fresh";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Inspect, register, or remove freshness policies";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Metadata";
  }

  /**
   * Register a given freshness policy in the metatable.
   *
   * @param tableName the name of the table
   * @param columnName the name of the column to which to attach the freshness policy.
   * @param producerClass the KijiProducer to run when a freshness policy triggers.
   * @param policy the KijiFreshnessPolicy to register.
   * @return the tool return code.
   * @throws IOException in case of an error writing to the metatable.
   */
  private int registerPolicy(
      String tableName,
      String columnName,
      Class<? extends KijiProducer> producerClass,
      KijiFreshnessPolicy policy)
      throws IOException {
    Map<ValidationFailure, Exception> failures;
    try {
      mManager.storePolicy(tableName, columnName, producerClass, policy);
      failures = null;
    } catch (FreshnessValidationException fve) {
      failures = fve.getExceptions();
    }

    if (failures == null) {
      if (isInteractive()) {
        getPrintStream().format("Freshness policy: %s with state: %s and producer: %s%n"
            + "attached to column: %s in table: %s%n",
            policy.getClass().getName(), policy.serialize(), producerClass.getName(),
            columnName, tableName);
      }
      return BaseTool.SUCCESS;
    } else {
      for (Map.Entry<ValidationFailure, Exception> entry : failures.entrySet()) {
        getPrintStream().format("%s: ", entry.getKey().toString());
        entry.getValue().printStackTrace(getPrintStream());
      }
      return BaseTool.FAILURE;
    }
  }

  /**
   * Register a freshness policy without checking for compatability.
   *
   * @param tableName the name of the table to which to register the policy.
   * @param columnName the name of the column to which to register the policy.
   * @param policyClass the KijiFreshnessPolicy class to register.
   * @param policyState the serialized state of the KijiFreshnessPolicy.
   * @param producerClass the KijiProducer class to run if data is stale.
   * @return the tool return code.
   * @throws IOException in case of an error writing to the metatable.
   */
  private int forceRegisterPolicy(
      final String tableName,
      final String columnName,
      final String policyClass,
      final String policyState,
      final String producerClass)
      throws IOException {
    Map<ValidationFailure, Exception> failures;
    try {
      mManager.storePolicyWithStrings(
          tableName, columnName, producerClass, policyClass, policyState);
      failures = null;
    } catch (FreshnessValidationException fve) {
      failures = fve.getExceptions();
    }

    if (failures == null) {
      if (isInteractive()) {
        getPrintStream().format("Freshness policy: %s with state: %s and producer: %s%n"
            + "attached to column: %s in table: %s without checks.%n",
            policyClass, policyState, producerClass,
            columnName, tableName);
      }
      return BaseTool.SUCCESS;
    } else {
      for (Map.Entry<ValidationFailure, Exception> entry : failures.entrySet()) {
        getPrintStream().format("%s: ", entry.getKey().toString());
        entry.getValue().printStackTrace(getPrintStream());
      }
      return BaseTool.FAILURE;
    }
  }

  /**
   * Unregister a given freshness policy from the metatable.
   *
   * @param tableName the name of the table from which to remove a freshness policy.
   * @param columnName the name of the column from which to remove a freshness policy.
   * @return the tool return code.
   * @throws IOException in case of an error writing to the metatable.
   */
  private int unregisterPolicy(String tableName, String columnName) throws IOException {
    mManager.removePolicy(tableName, columnName);
    if (isInteractive()) {
      getPrintStream().format("Freshness policy removed from column: %s in table %s%n",
          columnName, tableName);
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Retrieve and print the freshness policy associated with a given column.
   *
   * @param tableName the table containing the column to check.
   * @param columnName the name of the column for which to retrieve a freshness policy.
   * @return the tool return code.
   * @throws IOException in case of an error reading from the metatable.
   */
  private int retrievePolicy(String tableName, String columnName) throws IOException {
    final KijiFreshnessPolicyRecord record = mManager.retrievePolicy(tableName, columnName);
    if (record == null) {
      getPrintStream().format("There is no freshness policy attached to column: %s in table: %s%n",
          columnName, tableName);
    } else {
      getPrintStream().format(
          "Freshness policy class: %s%n"
          + "Freshness policy state: %s%n"
          + "Producer class: %s%n",
          record.getFreshnessPolicyClass(),
          record.getFreshnessPolicyState(),
          record.getProducerClass());
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Retrieve and print all freshness policies associated with a given table.
   *
   * @param tableName the name of the table for which to retrieve freshness policies.
   * @return the tool return code.
   * @throws IOException in case of an error reading from the metatable.
   */
  private int retrievePolicies(String tableName) throws IOException {
    final Map<KijiColumnName, KijiFreshnessPolicyRecord> records =
        mManager.retrievePolicies(tableName);
    if (records.isEmpty()) {
      getPrintStream().format("There are no freshness policies attached to columns in table: %s%n",
          tableName);
    } else {
      for (Map.Entry<KijiColumnName, KijiFreshnessPolicyRecord> entry : records.entrySet()) {
        final KijiFreshnessPolicyRecord record = entry.getValue();
        getPrintStream().format(
            "Freshness policy attached to column: %s%n"
            + "  Freshness policy class: %s%n"
            + "  Freshness policy state: %s%n"
            + "  Producer class: %s%n",
            entry.getKey().toString(),
            record.getFreshnessPolicyClass(),
            record.getFreshnessPolicyState(),
            record.getProducerClass());
      }
    }
    return BaseTool.SUCCESS;
  }

  /**
   * Validate that a freshness policy attached to a given column conforms to the restrictions
   * imposed on attachment.
   *
   * @param tableName the name of the table which contains the target column.
   * @param columnName the name of the column whose attached freshness policy should be validated.
   * @return the tool return code.
   * @throws IOException in case of an error reading from the metatable.
   */
  private int validatePolicy(String tableName, String columnName) throws IOException {
    final Map<ValidationFailure, Exception> failures =
        mManager.validatePolicy(tableName, columnName);
    if (failures.isEmpty()) {
      getPrintStream().format("Freshness Policy attached to column: %s in table: %s is valid.%n",
          columnName, tableName);
      return BaseTool.SUCCESS;
    } else {
      for (Map.Entry<ValidationFailure, Exception> entry : failures.entrySet()) {
        getPrintStream().format("%s: ", entry.getKey().toString());
        entry.getValue().printStackTrace(getPrintStream());
      }
      return BaseTool.FAILURE;
    }
  }

  /**
   * Validate that all freshness policies attached to a given table conform to the restrictions
   * imposed on attachment.
   *
   * @param tableName the name of the table whose attached freshness policies should be validated.
   * @return the tool return code.
   * @throws IOException in case of an error reading from the metatable.
   */
  private int validatePolicies(String tableName) throws IOException {
    final int numberOfPolicies = mManager.retrievePolicies(tableName).size();

    final Map<KijiColumnName, Map<ValidationFailure, Exception>> failures =
        mManager.validatePolicies(tableName);
    if (failures.isEmpty()) {
      getPrintStream().format("%d freshness policies found for table: %s%nAll freshness policies "
          + "are valid.%n", numberOfPolicies, tableName);
      return BaseTool.SUCCESS;
    } else {
      getPrintStream().format(
          "%d freshness policies found for table: %s%n", numberOfPolicies, tableName);
      for (Map.Entry<KijiColumnName, Map<ValidationFailure, Exception>> entry
          : failures.entrySet()) {
        getPrintStream().format("Freshness policy attached to column: %s is not valid.%n",
            entry.getKey().toString());
        for (Map.Entry<ValidationFailure, Exception> innerEntry : entry.getValue().entrySet()) {
          getPrintStream().format("%s: ", innerEntry.getKey().toString());
          innerEntry.getValue().printStackTrace(getPrintStream());
        }
      }
      return BaseTool.FAILURE;
    }
  }

  /**
   * Reads FreshnessPolicy state from a given file path String.
   *
   * @param path path to the freshness policy state file.
   * @return the sting contents of the freshness policy state.
   * @throws IOException in case of an error finding the file or reading from it.
   */
  private String readStateFromFile(String path) throws IOException {
    final FileInputStream input = new FileInputStream(path);
    final InputStreamReader inputReader = new InputStreamReader(input, "UTF-8");
    final BufferedReader bufferedReader = new BufferedReader(inputReader);
    try {
      final StringBuilder builder = new StringBuilder();
      String line;
      final String seperator = System.getProperty("line.separator");
      while ((line = bufferedReader.readLine()) != null) {
        builder.append(line);
        builder.append(seperator);
      }
      return builder.toString();
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(inputReader);
      IOUtils.closeQuietly(bufferedReader);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void validateFlags() throws Exception {
    Preconditions.checkArgument(null != mDoFlag && !mDoFlag.isEmpty(),
        "--do flag is required. Please specify exactly one of: register, unregister, retrieve, "
        + "retrieve-all, validate, validate-all");
    try {
      mDoMode = DoMode.valueOf(mDoFlag.toUpperCase(Locale.ROOT).replace("-", "_"));
    } catch (IllegalArgumentException iae) {
      getPrintStream().printf("Invalid --do command: '%s'.%n", mDoFlag);
      throw iae;
    }
    if (mDoMode == DoMode.REGISTER) {
      Preconditions.checkArgument(mPolicyClassFlag != null && !mPolicyClassFlag.isEmpty(),
          "--policy-class flag must be set to perform a freshness policy registration.");
      Preconditions.checkArgument(mProducerClassFlag != null && !mProducerClassFlag.isEmpty(),
          "--producer-class flag must be set to perform a freshness policy registration.");
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    Preconditions.checkNotNull(nonFlagArgs,
        "Specify a Kiji table or column with \"kiji fresh kiji://hbase-address/kiji-instance/"
        + "kiji-table/[optional-kiji-column]\"");
    Preconditions.checkArgument(nonFlagArgs.size() >= 1,
        "Specify a Kiji table or column with \"kiji fresh kiji://hbase-address/kiji-instance/"
            + "kiji-table/[optional-kiji-column]\"");
    try {
      mURI = KijiURI.newBuilder(nonFlagArgs.get(0)).build();
    } catch (KijiURIException kurie) {
      getPrintStream().format("Invalid KijiURI. Specify a Kiji table or column with \"kiji fresh"
          + " kiji://hbase-address/kiji-instance/kiji-table/[optional-kiji-column]\"");
      throw kurie;
    }
    if ((mDoMode == DoMode.RETRIEVE_ALL
        || mDoMode == DoMode.UNREGISTER_ALL
        || mDoMode == DoMode.VALIDATE_ALL)
        && (mURI.getTable() == null || mURI.getColumns().size() != 0)) {
      getPrintStream().format("Retrieve-all, unregister-all, and Validate-all requires a KijiURI "
          + "with a specified table and no specified columns.");
      return BaseTool.FAILURE;
    } else if (mDoMode != DoMode.RETRIEVE_ALL
        && mDoMode != DoMode.UNREGISTER_ALL
        && mDoMode != DoMode.VALIDATE_ALL
        && (mURI.getColumns() == null)) {
      getPrintStream().format("Retrieve, register, unregister, and validate require a KijiURI with "
          + "a specified column.");
      return BaseTool.FAILURE;
    }
    mKiji = Kiji.Factory.open(mURI);
    try {
      mManager = KijiFreshnessManager.create(mKiji);
      try {
        switch (mDoMode) {
          case RETRIEVE: {
            for (KijiColumnName column : mURI.getColumns()) {
              retrievePolicy(mURI.getTable(), column.getName());
            }
            return BaseTool.SUCCESS;
          }
          case RETRIEVE_ALL: {
            retrievePolicies(mURI.getTable());
            return BaseTool.SUCCESS;
          }
          case REGISTER: {
            Preconditions.checkArgument(!mURI.getColumns().isEmpty(), "Please specify at least one "
                + "column to register.");
            Preconditions.checkArgument((mPolicyStateFlag != null)
                ^ (mPolicyStateFileFlag != null && !mPolicyStateFileFlag.isEmpty()),
                "Specify only one of --policy-state and --policy-state-file.");
            final String policyState = (mPolicyStateFlag != null) ? mPolicyStateFlag
                : readStateFromFile(mPolicyStateFileFlag);
            boolean classesFound = true;
            KijiFreshnessPolicy policy = null;
            Class<? extends KijiProducer> producerClass = null;
            if (!mAsStringFlag) {
              try {
                policy = (KijiFreshnessPolicy) ReflectionUtils.newInstance(
                    Class.forName(mPolicyClassFlag), null);
                policy.deserialize(policyState);
              } catch (ClassNotFoundException cnfe) {
                classesFound = false;
                if (mayProceed("KijiFreshnessPolicy class: %s not found on the classpath.  Do you "
                    + "want to register this class name anyway?", mPolicyClassFlag)) {
                  mAsStringFlag = true;
                } else {
                  getPrintStream().println("Registration aborted.");
                  return BaseTool.FAILURE;
                }
              }
              try {
                producerClass =
                    Class.forName(mProducerClassFlag).asSubclass(KijiProducer.class);
              } catch (ClassNotFoundException cnfe) {
                classesFound = false;
                if (mayProceed("KijiProducer class: %s not found on the classpath.  Do you "
                    + "want to register this class name anyway?", mProducerClassFlag)) {
                  mAsStringFlag = true;
                } else {
                  getPrintStream().println("Registration aborted.");
                  return BaseTool.FAILURE;
                }
              }
            }
            if (classesFound && !mAsStringFlag) {
              for (KijiColumnName column : mURI.getColumns()) {
                if (mManager.retrievePolicy(mURI.getTable(), column.getName()) != null) {
                  if (mayProceed("There is already a freshness policy attached to column: %s in"
                      + "table: %s. Do you want to overwrite it?",
                      column.getName(), mURI.getTable())) {
                    registerPolicy(mURI.getTable(), column.getName(), producerClass, policy);
                  } else {
                    getPrintStream().println("Registration aborted.");
                    return BaseTool.FAILURE;
                  }
                } else {
                  registerPolicy(mURI.getTable(), column.getName(), producerClass, policy);
                }
              }
              return BaseTool.SUCCESS;
            } else {
              for (KijiColumnName column : mURI.getColumns()) {
                forceRegisterPolicy(mURI.getTable(), column.getName(),
                    mPolicyClassFlag, policyState, mProducerClassFlag);
              }
              return BaseTool.SUCCESS;
            }

          }
          case UNREGISTER: {
            for (KijiColumnName column : mURI.getColumns()) {
              unregisterPolicy(mURI.getTable(), column.getName());
            }
            return BaseTool.SUCCESS;
          }
          case UNREGISTER_ALL: {
            mManager.removePolicies(mURI.getTable());
            if (isInteractive()) {
              getPrintStream().format(
                  "All freshness policies removed from table: %s%n", mURI.getTable());
            }
            return BaseTool.SUCCESS;
          }
          case VALIDATE: {
            int retVal = BaseTool.SUCCESS;
            for (KijiColumnName column : mURI.getColumns()) {
              retVal = (validatePolicy(mURI.getTable(), column.getName()) == BaseTool.SUCCESS)
                  ? retVal : BaseTool.FAILURE;
            }
            return retVal;
          }
          case VALIDATE_ALL: {
            return validatePolicies(mURI.getTable());
          }
          default: {
            throw new InternalKijiError("Unsupported operation enum value.");
          }
        }
      } finally {
        mManager.close();
      }
    } finally {
      mKiji.release();
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
