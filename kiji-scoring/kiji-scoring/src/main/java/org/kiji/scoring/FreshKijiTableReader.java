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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.scoring.impl.InternalFreshKijiTableReader;
import org.kiji.scoring.statistics.FreshKijiTableReaderStatistics;

/**
 * Interface for reading freshened data from a Kiji Table.
 *
 * <p>
 *   Utilizes {@link org.kiji.schema.EntityId} and {@link org.kiji.schema.KijiDataRequest}
 *   to return {@link org.kiji.schema.KijiRowData}.
 * </p>
 * <p>
 *   Accessible via {@link org.kiji.scoring.FreshKijiTableReader.Builder#create()}.
 * </p>
 *
 * <p>
 *   Reads performed with FreshKijiTableReaders pass through freshness filters according to
 *   {@link org.kiji.scoring.KijiFreshnessPolicy}s registered in the
 *   {@link org.kiji.schema.KijiMetaTable} that services the table associated with this reader.
 * </p>
 *
 * <p>
 *   Freshening describes the process of conditionally applying a {@link ScoreFunction} to a row in
 *   response to user queries for data in that row.  Consequently, methods of a FreshKijiTableReader
 *   have the possibility of generating side effect writes to the rows users query.
 * </p>
 *
 * <p>
 *   FreshKijiTableReader get methods are used in the same way as regular KijiTableReader get
 *   methods.
 * </p>
 * <p>
 *   To get the three most recent versions of cell data from a column <code>bar</code> from
 *   the family <code>foo</code>:
 * <pre>
 *   KijiDataRequestBuilder builder = KijiDataRequest.builder()
 *     .newColumnsDef()
 *     .withMaxVersions(3)
 *     .add("foo", "bar");
 *   final KijiDataRequest request = builder.build();
 *
 *   final KijiTableReader freshReader = Builder.create()
 *       .withTable(table)
 *       .withTimeout(100)
 *       .build();
 *   final KijiRowData data = freshReader.get(myEntityId, request);
 * </pre>
 *   This code will return the three most recent values including newly generated values output by
 *   the ScoreFunction if it ran.
 * </p>
 *
 * <p>
 *   Instances of this reader are thread safe and may be used across multiple threads. Because this
 *   class maintains a connection to the underlying KijiTable and other resources, users should call
 *   {@link #close()} when done using a reader.
 * </p>
 *
 * @see org.kiji.scoring.KijiFreshnessPolicy
 * @see org.kiji.scoring.ScoreFunction
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
public interface FreshKijiTableReader extends KijiTableReader {

  /**
   * Builder for configuring options for FreshKijiTableReaders.
   *
   * <p>
   *   Allows the setting of options for creation of FreshKijiTableReaders.  Options include:
   *   <ul>
   *     <li>A required KijiTable from which to read.</li>
   *     <li>
   *       Setting the default time in milliseconds to wait for freshening to occur (defaults to
   *       100)
   *     </li>
   *     <li>
   *       Setting the period in milliseconds between automatically rereading Freshener records from
   *       the meta table (defaults to never automatically rereading).
   *     </li>
   *     <li>
   *       Setting whether to allow partially fresh data to be written and returned by calls to
   *       {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} (defaults to not
   *       allowing partial freshning).
   *     </li>
   *     <li>Setting which columns will be freshened by this reader (defaults to all columns).</li>
   *     <li>
   *       Setting what level of statistics to gather and how often they should be logged (defaults
   *       to gathering no statistics with no logging interval).
   *     </li>
   *   </ul>
   * </p>
   *
   * <p>
   *   To create a new FreshKijiTableReader:
   * </p>
   * <p><pre>
   *   final FreshKijiTableReader = FreshKijiTableReader.Builder.create()
   *       .withTable(myTable)
   *       .withTimeout(100)
   *       .withAutomaticReread(3600000)
   *       .returnPartialFreshData(true)
   *       build();
   * </pre></p>
   *
   * <p>Instance of this builder are not thread safe.</p>
   */
  @ApiStability.Experimental
  public static final class Builder {
    /** By default, do not allow returning partially fresh data. */
    private static final Boolean DEFAULT_PARTIAL_FRESHENING = false;
    /** By default, Wait 100 milliseconds for freshening to occur. */
    private static final long DEFAULT_TIMEOUT = 100;
    /** By default, do not automatically reread. */
    private static final long DEFAULT_REREAD_PERIOD = 0;
    /** By default, freshen all columns. */
    private static final List<KijiColumnName> DEFAULT_COLUMNS_TO_FRESHEN = Collections.emptyList();
    /** By default, do not gather statistics. */
    private static final StatisticGatheringMode DEFAULT_STATISTICS_MODE =
        StatisticGatheringMode.NONE;
    /** By default, log statistics every 10 minutes. */
    private static final long DEFAULT_STATISTICS_LOGGING_INTERVAL = 10 * 60 * 1000;
    /** Enumeration of possible modes of statistics gathering. */
    public static enum StatisticGatheringMode {
      NONE, ALL
    }

    /**
     * Get a new instance of Builder.
     *
     * @return a new instance of Builder.
     */
    public static Builder create() {
      return new Builder();
    }

    /** The KijiTable from which the new reader will read. */
    private KijiTable mTable = null;
    /**
     * The time in milliseconds the new reader will wait for freshening to occur. Default is 100
     * milliseconds.
     */
    private Long mTimeout = null;
    /**
     * The time in milliseconds the new reader will wait between automatically rereading Freshener
     * records from the meta table. Default is to not automatically reread.
     */
    private Long mRereadPeriod = null;
    /**
     * Whether or not the new reader will return and commit partially fresh data when available.
     * Default is to not allow partial freshening.
     */
    private Boolean mAllowPartialFresh = null;
    /** Specifies which columns to freshen.  Default is all columns. */
    private List<KijiColumnName> mColumnsToFreshen = null;
    /** Specifies what statistics to gather. */
    private StatisticGatheringMode mStatisticGatheringMode = null;
    /** Time in milliseconds between logging collected statistics. */
    private Long mStatisticsLoggingInterval = null;

    /**
     * Configure the FreshKijiTableReader to read from the given KijiTable.
     *
     * @param table the KijiTable from which to read.
     * @return this Builder configured to read from the given table.
     */
    public Builder withTable(
        final KijiTable table
    ) {
      Preconditions.checkNotNull(table, "Specified KijiTable may not be null.");
      Preconditions.checkState(null == mTable, "KijiTable already set to: %s", mTable);
      mTable = table;
      return this;
    }

    /**
     * Get the configured table from this builder or null if the table has not been specified.
     *
     * @return the configured table from this builder or null if the table has not been specified.
     */
    public KijiTable getTable() {
      return mTable;
    }

    /**
     * Configure the FreshKijiTableReader to wait a given number of milliseconds before returning
     * stale data.
     *
     * @param timeout the default duration in milliseconds to wait before returning stale data. This
     *     may be overriden at request time by using
     *     {@link FreshKijiTableReader#get(org.kiji.schema.EntityId,
     *     org.kiji.schema.KijiDataRequest, FreshRequestOptions)}.
     * @return this Builder configured to wait the given number of milliseconds
     * before returning stale data.
     */
    public Builder withTimeout(
        final long timeout
    ) {
      Preconditions.checkArgument(0 < timeout, "Timeout must be positive, got: %d", timeout);
      Preconditions.checkState(null == mTimeout, "Timeout is already set to: %d", mTimeout);
      mTimeout = timeout;
      return this;
    }

    /**
     * Get the configured timeout from this builder or null if the timeout has not been specified.
     *
     * @return the configured timeout from this builder or null if the timeout has not been
     *     specified.
     */
    public long getTimeout() {
      return mTimeout;
    }

    /**
     * Configure the FreshKijiTableReader to automatically reread freshness policies from the meta
     * table on a scheduled interval.
     *
     * @param rereadPeriod the interval between automatic rereads in milliseconds.  rereadPeriod may
     * not be negative and a rereadPeriod value of 0 indicate never automatically rereading
     * freshness policies from the metatable.
     * @return this Builder configured to automatically reread on the given
     * interval.
     */
    public Builder withAutomaticReread(
        final long rereadPeriod
    ) {
      Preconditions.checkArgument(
          0 < rereadPeriod, "Reread time must be positive, got: %s", rereadPeriod);
      Preconditions.checkState(
          null == mRereadPeriod, "Reread time is already set to: %d", mRereadPeriod);
      mRereadPeriod = rereadPeriod;
      return this;
    }

    /**
     * Get the configured automatic reread period or null if none has been set. If this value is 0,
     * this indicates that automatic rereading has been set to disabled.
     *
     * @return the configured automatic reread period or null if none has been set.
     */
    public long getAutomaticRereadPeriod() {
      return mRereadPeriod;
    }

    /**
     * Configure the FreshKijiTableReader to return partially fresh data when available.  This
     * option may increase the time to return for certain calls to
     * {@link FreshKijiTableReader#get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)}.
     * If set to true, each producer will create its own table connection and all producer writes
     * will be committed atomically when produce() returns.  If set to false, all producers for a
     * single request will share a table connection and all writes will be cached until all
     * producers for that request have returned.
     *
     * @param allowPartial whether the FreshKijiTableReader should return partially freshened data
     * when available.  If set to true, each producer will create its own table connection and all
     * producer writes will be committed atomically when produce() returns.  If set to false, all
     * producers for a single request will share a table connection and all writes will be cached
     * until all producers for that request have returned.
     * @return this Builder configured to allow returning partially freshened
     * data.
     */
    public Builder withPartialFreshening(
        final boolean allowPartial
    ) {
      Preconditions.checkState(null == mAllowPartialFresh,
          "Partial freshening is already set to: %s", mAllowPartialFresh);
      mAllowPartialFresh = allowPartial;
      return this;
    }

    /**
     * Get the configured partial freshening or null if none has been set.
     *
     * @return the configured partial freshening or null if none has been set.
     */
    public boolean getPartialFreshening() {
      return mAllowPartialFresh;
    }

    /**
     * Configure the FreshKijiTableReader to only freshen requests for a specific set of columns.
     *
     * <ul>
     *   <li>Specifying a qualified column will enable the Freshener for that column only.</li>
     *   <li>
     *     Specifying a column family will enable the Fresheners for all columns in that family.
     *   </li>
     * </ul>
     *
     * @param columnsToFreshen the columns which the reader should freshen.
     * @return this Builder configured to read from a specific set of columns.
     */
    public Builder withColumnsToFreshen(
        final List<KijiColumnName> columnsToFreshen
    ) {
      if (null != mColumnsToFreshen) {
        final String columns = Joiner.on(", ").join(mColumnsToFreshen);
        throw new IllegalStateException(
            String.format("Columns to freshen are already set to: %s", columns));
      } else {
        mColumnsToFreshen = columnsToFreshen;
      }
      return this;
    }

    /**
     * Get the configured columns to freshen or null if none have been set. The list returned by
     * this method is immutable. Any attempt to mutate it will throw an exception.
     *
     * @return the configured columns to freshen or null if none have been set.
     */
    public List<KijiColumnName> getColumnsToFreshen() {
      return ImmutableList.copyOf(mColumnsToFreshen);
    }

    /**
     * Configure the FreshKijiTableReader to gather statistics about freshening requests.
     *
     * <p>Modes:</p>
     * <ul>
     *   <li>NONE - gather no statistics.</li>
     *   <li>ALL - preserve all statistics, aggregates and all individual request statistics will be
     *       saved.</li>
     * </ul>
     *
     * @param mode the statistics gathering mode.
     * @param loggingInterval time in milliseconds between logging collected statistics. 0 indicates
     *     no automatic logging.
     * @return this Builder configured to collect and log statistics.
     */
    public Builder withStatisticsGathering(
        final StatisticGatheringMode mode,
        final long loggingInterval
    ) {
      Preconditions.checkState(null == mStatisticGatheringMode,
          "Statistics gathering mode is already set to: %s", mStatisticGatheringMode);
      Preconditions.checkArgument(0 <= loggingInterval,
          "Logging interval must be greater than or equal to 0. 0 indicates no logging.");
      mStatisticGatheringMode = mode;
      mStatisticsLoggingInterval = loggingInterval;
      return this;
    }

    /**
     * Get the configured statistics gathering mode or null if none has been set.
     *
     * @return the configured statistics gathering mode or null if none has been set.
     */
    public StatisticGatheringMode getStatisticGatheringMode() {
      return mStatisticGatheringMode;
    }

    /**
     * Get the statistics logging interval or null if none has been set.
     *
     * @return the statistics logging interval or null if none has been set.
     */
    public long getStatisticsLoggingInterval() {
      return mStatisticsLoggingInterval;
    }

    /**
     * Builds a FreshKijiTableReader with the configured options.
     *
     * @return a FreshKijiTableReader with the configured options.
     * @throws IOException in case of an error creating the FreshKijiTableReader.
     */
    public FreshKijiTableReader build() throws IOException {
      Preconditions.checkState(null != mTable, "Target table must be set in order to build.");
      if (null == mTimeout) {
        mTimeout = DEFAULT_TIMEOUT;
      }
      if (null == mRereadPeriod) {
        mRereadPeriod = DEFAULT_REREAD_PERIOD;
      }
      if (null == mAllowPartialFresh) {
        mAllowPartialFresh = DEFAULT_PARTIAL_FRESHENING;
      }
      if (null == mColumnsToFreshen) {
        mColumnsToFreshen = DEFAULT_COLUMNS_TO_FRESHEN;
      }
      if (null == mStatisticGatheringMode) {
        mStatisticGatheringMode = DEFAULT_STATISTICS_MODE;
        mStatisticsLoggingInterval = DEFAULT_STATISTICS_LOGGING_INTERVAL;
      }

      return new InternalFreshKijiTableReader(
          mTable,
          mTimeout,
          mRereadPeriod,
          mAllowPartialFresh,
          mColumnsToFreshen,
          mStatisticGatheringMode,
          mStatisticsLoggingInterval);
    }
  }

  /**
   * Options which affect the behavior of a single freshening request. Used via
   * {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest,
   * org.kiji.scoring.FreshKijiTableReader.FreshRequestOptions)}
   */
  @ApiStability.Experimental
  public static final class FreshRequestOptions {

    /** Builder for FreshRequestOptions. Instance of this builder are not thread safe. */
    @ApiStability.Experimental
    public static final class Builder {

      /** -1 is a code for the FreshKijiTableReader to use its configured default timeout. */
      private static final long DEFAULT_TIMEOUT = -1;
      private static final Map<String, String> DEFAULT_PARAMETERS = Collections.emptyMap();

      private Long mTimeout = null;
      private Map<String, String> mParameters = null;

      /** Private constructor. */
      private Builder() { }

      /**
       * Create a new FreshRequestOptions.Builder.
       *
       * @return a new FreshRequestOptions.Builder.
       */
      public static Builder create() {
        return new Builder();
      }

      /**
       * Configure the FreshRequestOptions to include the given timeout.
       *
       * @param timeout time in milliseconds to wait for this freshening request to finish.
       * @return this builder configured to include the given timeout.
       */
      public Builder withTimeout(
          final long timeout
      ) {
        Preconditions.checkArgument(0 < timeout, "Timeout must be positive, got: %s", timeout);
        Preconditions.checkState(null == mTimeout, "Timeout is already set to: %s", mTimeout);
        mTimeout = timeout;
        return this;
      }

      /**
       * Get the configured timeout or null if none has been set.
       *
       * @return the configured timeout or null if none has been set.
       */
      public long getTimeout() {
        return mTimeout;
      }

      /**
       * Configure the FreshRequestOptions to include the given configuration parameters.
       *
       * @param parameters configuration parameters to include in the FreshRequestOptions.
       * @return this builder configured to include the given configuration parameters.
       */
      public Builder withParameters(
          final Map<String, String> parameters
      ) {
        Preconditions.checkNotNull(parameters, "Parameters may not be null.");
        Preconditions.checkState(
            null == mParameters, "Parameters are already set to: %s", mParameters);
        mParameters = Maps.newHashMap(parameters);
        return this;
      }

      /**
       * Add the given parameters to the set of parameters included in this FreshRequestOptions.
       *
       * @param parameters configuration parameters to add. These values will override existing
       *     values in this builder.
       * @return this builder configured to include the given configuration parameters.
       */
      public Builder addParameters(
          final Map<String, String> parameters
      ) {
        Preconditions.checkNotNull(parameters, "Parameters may not be null.");
        if (null == mParameters) {
          mParameters = Maps.newHashMap(parameters);
        } else {
          mParameters.putAll(parameters);
        }
        return null;
      }

      /**
       * Get the configured parameters or null if none have been set. The map returned by this
       * method is immutable. Any attempt to mutate it will throw an exception.
       *
       * @return the configured parameters or null if none have been set.
       */
      public Map<String, String> getParameters() {
        return ImmutableMap.copyOf(mParameters);
      }

      /**
       * Build a FreshRequestOptions from the configured state and default values.
       *
       * @return a new FreshRequestOptions from the configured state and default values.
       */
      public FreshRequestOptions build() {
        if (null == mTimeout) {
          mTimeout = DEFAULT_TIMEOUT;
        }
        if (null == mParameters) {
          mParameters = DEFAULT_PARAMETERS;
        }

        return new FreshRequestOptions(mTimeout, mParameters);
      }
    }

    /**
     * Convenience method for creating a FreshRequestOptions with the given timeout and default
     * parameters. Default parameters are empty.
     *
     * @param timeout time in milliseconds to wait for this freshening request to finish.
     * @return a new FreshRequestOptions with the given timeout and default parameters.
     */
    public static FreshRequestOptions withTimeout(
        final long timeout
    ) {
      return Builder.create().withTimeout(timeout).build();
    }

    /**
     * Convenience method for creating a FreshRequestOptions with the given parameters and default
     * timeout.
     *
     * @param parameters configuration parameters which will be available to all Fresheners run in
     *     response to this request.
     * @return a new FreshRequestOptions with the given parameters and default timeout.
     */
    public static FreshRequestOptions withParameters(
        final Map<String, String> parameters
    ) {
      return Builder.create().withParameters(parameters).build();
    }

    private final long mTimeout;
    private final Map<String, String> mParameters;

    /**
     * Initialize a new FreshRequestOptions with the given timeout and parameters.
     *
     * @param timeout time in milliseconds to wait for this freshening request to finish.
     * @param parameters configuration parameters which will be available to all Fresheners run in
     *     response to this request.
     */
    private FreshRequestOptions(
        final long timeout,
        final Map<String, String> parameters
    ) {
      mTimeout = timeout;
      mParameters = parameters;
    }

    /**
     * Get the timeout from this FreshRequestOptions.
     *
     * @return the timeout from this FreshRequestOptions.
     */
    public long getTimeout() {
      return mTimeout;
    }

    /**
     * Get configuration parameters from this FreshRequestOptions.
     *
     * @return configuration parameters from this FreshRequestOptions.
     */
    public Map<String, String> getParameters() {
      return mParameters;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(FreshRequestOptions.class)
          .add("timeout", mTimeout)
          .add("parameter_overrides", mParameters)
          .toString();
    }
  }

  /**
   * Freshens data as needed before returning. If freshening has not completed within the
   * configured timeout, will return stale or partially freshened data depending on the
   * configuration of the reader.  Behaves the same as
   * {@link org.kiji.schema.KijiTableReader#get(org.kiji.schema.EntityId,
   * org.kiji.schema.KijiDataRequest)} except for the possibility of freshening.
   *
   * @param entityId EntityId of the row to query.
   * @param dataRequest What data to retrieve.
   * @return The data requested after freshening.
   * @throws IOException in case of an error reading from the table.
   */
  @Override
  KijiRowData get(EntityId entityId, KijiDataRequest dataRequest) throws IOException;

  /**
   * Freshens data as needed before returning. If freshening has not completed within the specified
   * timeout, will return stale or partially freshened data depending on the configuration of the
   * reader.  Behaves the same as
   * {@link org.kiji.schema.KijiTableReader#get(org.kiji.schema.EntityId,
   * org.kiji.schema.KijiDataRequest)} except for the possibility of freshening.
   *
   * @param entityId the EntityId of the row to query.
   * @param dataRequest what data to retrieve.
   * @param options options which affect the behavior of this freshening request only.
   * @return the data requested after freshening.
   * @throws IOException in case of an error reading from the table.
   */
  KijiRowData get(EntityId entityId, KijiDataRequest dataRequest, FreshRequestOptions options)
      throws IOException;

  /**
   * Attempts to freshen all data requested in parallel before returning. If freshening has not
   * completed within the configured timeout, will return stale or partially freshened data
   * depending on the configuration of the reader.
   *
   * @param entityIds A list of EntityIds for the rows to query.
   * @param dataRequest What data to retrieve from each row.
   * @return a list of KijiRowData corresponding to the EntityIds and data request after
   *   freshening.
   * @throws IOException in case of an error reading from the table.
   */
  @Override
  List<KijiRowData> bulkGet(List<EntityId> entityIds, KijiDataRequest dataRequest)
      throws IOException;

  /**
   * Attempts to freshen all data requested in parallel before returning.  If freshening has not
   * completed with the specified timeout, will return stale or partially freshened data depending
   * on the configuration of the reader.
   *
   * @param entityIds a list of EntityIds for the rows to query.
   * @param dataRequest what data to retrieve from each row.
   * @param options options which affect the behavior of this freshening request only.
   * @return a list of KijiRowData corresponding to the EntityIds and data request after freshening.
   * @throws IOException in case of an error reading from the table.
   */
  List<KijiRowData> bulkGet(
      List<EntityId> entityIds,
      KijiDataRequest dataRequest,
      FreshRequestOptions options
  ) throws IOException;

  /**
   * Clear cached Fresheners and reload from the meta table. This method replaces only those
   * Fresheners which have changed since the last call to rereadFreshenerRecords() or the
   * construction of the reader.
   *
   * @throws IOException in case of an error reading from the meta table.
   */
  void rereadFreshenerRecords() throws IOException;

  /**
   * Clear cached Fresheners and reload from the meta table. Replaces existing list of
   * columns to freshen with the given list and instantiates any Fresheners applicable to added
   * columns. This method replaces only those Fresheners which have changed since the last call to
   * rereadFreshenerRecords or the construction of the reader.
   *
   * @param columnsToFreshen the new set of columnsToFreshen.  This list will replace the previous
   *     list permanently.
   * @throws IOException in case of an error reading from the meta table.
   */
  void rereadFreshenerRecords(List<KijiColumnName> columnsToFreshen) throws IOException;

  /**
   * Get all statistics gathered by this reader about its Fresheners.
   *
   * @return all statistics gathered by this reader about its Fresheners.
   */
  FreshKijiTableReaderStatistics getStatistics();
}
