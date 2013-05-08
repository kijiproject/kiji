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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Preconditions;
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
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * Command line interface tool for registering and inspecting freshness policies.
 *
 * Usage:
 *  <pre>
 *  // Print all freshness policies attached to a table
 *  kiji fresh kiji://.env/instance/table --do=retrieve-all
 *  // Print the freshness policy attached to a column.  (If multiple columns are specified, will
 *  // print each freshness policy)
 *  kiji fresh kiji://.env/instance/table/family:qualifier --do=retrieve
 *  // Register a freshness policy for a column
 *  kiji fresh kiji://.env/instance/table/family:qualifer --do=register \
 *    org.kiji.scoring.lib.ShelfLife 10 com.mycompany.freshening.RecommendingProducer
 *  // Unregister a freshness policy from a column
 *  kiji fresh kiji://.env/instance/table/family:qualifier --do=unregister
 *  </pre>
 */
public class FreshTool extends BaseTool {
  private static final Logger LOG = LoggerFactory.getLogger(FreshTool.class);

  // Positional argument for the table or column

  @Flag(name="do", required=true, usage=
      "\"register <policy class> <policy state> <producer class>\"; "
      + "\"unregister\"; "
      + "\"retrieve\"; "
      + "\"retrieve-all\"")
  private String mDoFlag = "";

  @Flag(name="force", usage="set to true to write strings directly without checking for "
      + "compatability")
  private Boolean mForceFlag = false;

  /** URI of the Kiji table or column on which to perform an operation. */
  private KijiURI mURI = null;

  /** Kiji instance housing the metatable for the target KijiTable or column. */
  private Kiji mKiji = null;

  /** KijiFreshnessManager for the target table. */
  private KijiFreshnessManager mManager = null;

  /** Operation selector mode. */
  private static enum DoMode {
    REGISTER, UNREGISTER, RETRIEVE, RETRIEVE_ALL, UNREGISTER_ALL
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
    mManager.storePolicy(tableName, columnName, producerClass, policy);
    if (isInteractive()) {
      getPrintStream().format("Freshness policy: %s with state: %s and producer: %s%n"
          + "attached to column: %s in table: %s",
          policy.getClass().getName(), policy.serialize(), producerClass.getName(),
          columnName, tableName);
    }
    return BaseTool.SUCCESS;
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
    mManager.storePolicyWithStrings(
        tableName, columnName, producerClass, policyClass, policyState);
    if (isInteractive()) {
      getPrintStream().format("Freshness policy: %s with state: %s and producer: %s%n "
          + "attached to column: %s in table: %s without checks.",
          policyClass, policyState, producerClass,
          columnName, tableName);
    }
    return BaseTool.SUCCESS;
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
      getPrintStream().format("Freshness policy removed from column: %s in table %s",
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
      getPrintStream().format("There is no freshness policy attached to column: %s in table: %s",
          columnName, tableName);
    } else {
      getPrintStream().format(
          "Freshness policy class: %s%n"
          + "Freshness policy state: %s%n"
          + "Producer class: %s",
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
    if (records.size() == 0) {
      getPrintStream().format("There are no freshness policies attached to columns in table: %s",
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

  /** {@inheritDoc} */
  @Override
  public void validateFlags() throws Exception {
    try {
      mDoMode = DoMode.valueOf(mDoFlag.toUpperCase(Locale.ROOT).replace("-", "_"));
    } catch (IllegalArgumentException iae) {
      getPrintStream().printf("Invalid --do command: '%s'.%n", mDoFlag);
      throw iae;
    }
  }

  /** {@inheritDoc} */
  @Override
  protected int run(final List<String> nonFlagArgs) throws Exception {
    Preconditions.checkNotNull(nonFlagArgs,
        "Specify a Kiji table or column with \"kiji fresh kiji://hbase-address/kiji-instance/"
        + "kiji-table/[optional-kiji-column]\"");
    try {
      mURI = KijiURI.newBuilder(nonFlagArgs.get(0)).build();
    } catch (KijiURIException kurie) {
      getPrintStream().format("Invalid KijiURI. Specify a Kiji table or column with \"kiji fresh"
          + " kiji://hbase-address/kiji-instance/kiji-table/[optional-kiji-column]\"");
      throw kurie;
    }
    if ((mDoMode == DoMode.RETRIEVE_ALL || mDoMode == DoMode.UNREGISTER_ALL)
        && (mURI.getTable() == null || mURI.getColumns().size() != 0)) {
      getPrintStream().format("Retrieve-all requires a KijiURI with a specified table and no "
          + "specified columns.");
      return BaseTool.FAILURE;
    } else if (mDoMode != DoMode.RETRIEVE_ALL && mDoMode != DoMode.UNREGISTER_ALL
        && (mURI.getColumns() == null)) {
      getPrintStream().format("Retrieve, register, and unregister require a KijiURI with a "
          + "specified column.");
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
            Preconditions.checkArgument(nonFlagArgs.size() == 4, "Incorrect number of arguments "
                + "for register operation.  specify \"do=register <policy class> '<policy state>' "
                + "<producer class>\"");
            boolean classesFound = true;
            KijiFreshnessPolicy policy = null;
            Class<? extends KijiProducer> producerClass = null;
            if (!mForceFlag) {
              try {
                policy = (KijiFreshnessPolicy) ReflectionUtils.newInstance(
                    Class.forName(nonFlagArgs.get(1)), null);
                policy.deserialize(nonFlagArgs.get(2));
              } catch (ClassNotFoundException cnfe) {
                classesFound = false;
                if (mayProceed("KijiFreshnessPolicy class: %s not found on the classpath.  Do you "
                    + "want to register this class name anyway?", nonFlagArgs.get(1))) {
                  mForceFlag = true;
                } else {
                  getPrintStream().println("Registration aborted.");
                  return BaseTool.FAILURE;
                }
              }
              try {
                producerClass =
                    Class.forName(nonFlagArgs.get(3)).asSubclass(KijiProducer.class);
              } catch (ClassNotFoundException cnfe) {
                classesFound = false;
                if (mayProceed("KijiProducer class: %s not found on the classpath.  Do you "
                    + "want to register this class name anyway?", nonFlagArgs.get(3))) {
                  mForceFlag = true;
                } else {
                  getPrintStream().println("Registration aborted.");
                  return BaseTool.FAILURE;
                }
              }
            }
            if (classesFound && !mForceFlag) {
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
                    nonFlagArgs.get(1), nonFlagArgs.get(2), nonFlagArgs.get(3));
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
                  "All freshness policies removed from table: %s", mURI.getTable());
            }
            return BaseTool.SUCCESS;
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
}
