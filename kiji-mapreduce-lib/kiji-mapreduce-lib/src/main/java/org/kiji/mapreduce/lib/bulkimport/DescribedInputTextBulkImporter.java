/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.lib.bulkimport;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.mapreduce.framework.JobHistoryCounters;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * DescribedInputTextBulkImporter is an abstract class that provides methods to bulk importers for
 * mapping from source fields in the import lines to destination Kiji columns.  These can be used
 * inside of KijiBulkImportJobBuilder via the withBulkImporter method.
 *
 * Importing from a text file requires specifying a KijiColumnName, and the source field
 * for each element to be inserted into Kiji, in addition to the raw import data.  This information
 * is provided by {@link KijiTableImportDescriptor} which is set via
 * the <code>kiji.import.text.input.descriptor.path</code> parameter in {@link #CONF_FILE}.
 *
 * <p>Use this Mapper over text files to import data into a Kiji
 * table.  Each line in the file will be treated as data for one row.
 * This line should generate a single EntityId to write to, and any number
 * of writes to add to that entity.  You should override the produce(String, Context)
 * method to generate the entities from the input lines.</p>
 *
 * Extensions of this class should implement the following methods:
 * <ul>
 *   <li>{@link #produce} - actual producer code for the bulk importer should go here</li>
 *   <li>{@link #setupImporter} - (optional) any specific setup for this bulk importer.</li>
 *   <li>{@link #cleanupImporter} - (optional) any specific cleanup for this bulk importer.</li>
 * </ul>
 *
 * Extensions of this class can use the following methods to implement their producers:
 * <ul>
 *   <li>{@link #convert} - parses the text into the type associated with the column.</li>
 *   <li>{@link #incomplete} - to log and mark a row that was incomplete.</li>
 *   <li>{@link #reject} - to log and mark a row that could not be processed.</li>
 *   <li>{@link #getDestinationColumns} - to retrieve a collection of destination columns.</li>
 *   <li>{@link #getSource} - to retrieve the source for one of the columns listed above.</li>
 *   <li>{@link #getEntityIdSource()} - to retrieve the source for the entity id for the row.</li>
 * </ul>
 */

@ApiAudience.Public
@Inheritance.Extensible
public abstract class DescribedInputTextBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(DescribedInputTextBulkImporter.class);

  /**
   * Location of writer layout file.  File names columns and schemas, and implies
   * ordering of columns in delimited read-in file.
   */
  public static final String CONF_FILE = "kiji.import.text.input.descriptor.path";

  public static final String CONF_LOG_RATE = "kiji.import.text.log.rate";

  private static final ImmutableMap<String, Class<?>> KIJI_CELL_TYPE_TO_CLASS_MAP =
      new ImmutableMap.Builder<String, Class<?>>()
          .put("\"boolean\"", Boolean.class)
          .put("\"int\"", Integer.class)
          .put("\"long\"", Long.class)
          .put("\"float\"", Float.class)
          .put("\"double\"", Double.class)
          .put("\"string\"", String.class)
          .build();

  // Number of lines to skip between reject/incomplete lines
  private Long mLogRate = 1000L;

  // Current counter of the number of incomplete lines.
  private Long mIncompleteLineCounter = 0L;

  // Current counter of the number of rejected lines.
  private Long mRejectedLineCounter = 0L;

  /** Table layout of the output table. */
  private KijiTableLayout mOutputTableLayout;

  /** Table import descriptor for this bulk load. */
  private KijiTableImportDescriptor mTableImportDescriptor;

  /** KijiColumnName to cell type map. */
  private Map<KijiColumnName, Class> mColumnNameClassMap;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
  }

  /**
   * Performs validation that this table import descriptor can be applied to the output table.
   *
   * This method is final to prevent it from being overridden without being called.
   * Subclasses should override the setupImporter() method instead of overriding this method.
   *
   * {@inheritDoc}
   */
  @Override
  public final void setup(KijiTableContext context) throws IOException {
    HadoopConfigurator.configure(this);
    final Configuration conf = getConf();
    Preconditions.checkNotNull(mTableImportDescriptor);

    final KijiURI uri = KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build();

    final Kiji kiji = Kiji.Factory.open(uri, conf);
    try {
      final KijiTable table = kiji.openTable(uri.getTable());
      try {
        mOutputTableLayout = table.getLayout();
      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }

    Preconditions.checkNotNull(mOutputTableLayout);
    mTableImportDescriptor.validateDestination(mOutputTableLayout);

    // Retrieve the classes for all of the imported columns.
    Map<KijiColumnName, Class> columnNameClassMap = Maps.newHashMap();
    for (KijiColumnName kijiColumnName : mTableImportDescriptor.getColumnNameSourceMap().keySet()) {
      CellSchema cellSchema = mOutputTableLayout.getCellSchema(kijiColumnName);
      switch(cellSchema.getType()) {
        case INLINE:
          if (KIJI_CELL_TYPE_TO_CLASS_MAP.containsKey(cellSchema.getValue())) {
            columnNameClassMap.put(kijiColumnName,
              KIJI_CELL_TYPE_TO_CLASS_MAP.get(cellSchema.getValue()));
          } else {
            throw new IOException("Unsupported described output type: " + cellSchema.getValue());
          }
          break;
        case CLASS:
          throw new IOException("Unsupported described output type: " + cellSchema.getType());
        default:
          throw new IOException("Unsupported described output type: " + cellSchema.getType());
      }
    }
    mColumnNameClassMap = ImmutableMap.copyOf(columnNameClassMap);

    setupImporter(context);
  }

  /**
   * Extensible version of {@link KijiBulkImporter#setup} for subclasses of
   * DescribedInputTextBulkImporter.
   * Does nothing by default.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   * @throws IOException on I/O error.
   */
  public void setupImporter(KijiTableContext context) throws IOException {}

  /**
   * Converts a line of text to a set of writes to <code>context</code>, and
   * an EntityId for the row.
   *
   * @param line The line to parse.
   * @param context The context to write to.
   * @throws IOException if there is an error.
   */
  public abstract void produce(Text line, KijiTableContext context)
      throws IOException;

  /**
   * Post-processes incomplete lines(Logging, keeping count, etc).
   *
   * @param line the line that was marked incomplete incomplete by the producer.
   * @param context the context in which the incompletion occured.
   * @param reason the reason why this line was incomplete.
   */
  public void incomplete(Text line, KijiTableContext context, String reason) {
    if (mIncompleteLineCounter % mLogRate == 0L) {
      LOG.error("Incomplete line: {} with reason: {}",
          line.toString(), reason);
    }
    mIncompleteLineCounter++;

    //TODO(KIJIMRLIB-9) Abort this bulk importer job early if incomplete records exceed a threshold
    context.incrementCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_INCOMPLETE);

    //TODO(KIJIMRLIB-4) Add a strict mode where we reject incomplete lines
  }

  /**
   * Post-processes rejected lines(Logging, keeping count, etc).
   *
   * @param line the line that was rejected by the producer.
   * @param context the context in which the rejection occured.
   * @param reason the reason why this line was rejected.
   */
  public void reject(Text line, KijiTableContext context, String reason) {
    if (mRejectedLineCounter % mLogRate == 0L) {
      LOG.error("Rejecting line: {} with reason: {}",
          line.toString(), reason);
    }
    mRejectedLineCounter++;

    //TODO(KIJIMRLIB-9) Abort this bulk importer job early if rejected records exceed a threshold
    context.incrementCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_REJECTED);

    //TODO(KIJIMRLIB-4) Allow this to emit to a rejected output so that import can be reattempted.
  }

  /**
   * Converts the value into an object of the type associated with the specified column.
   * @param kijiColumnName the destination column to infer the type from.
   * @param value string representation of the value.
   * @return object containing the parsed representation of the value.
   */
  public Object convert(KijiColumnName kijiColumnName, String value) {
    Class<?> clazz = mColumnNameClassMap.get(kijiColumnName);
    if (clazz == Boolean.class) {
      return Boolean.valueOf(value);
    } else if (clazz == Integer.class) {
      return Integer.valueOf(value);
    } else if (clazz == Long.class) {
      return Long.valueOf(value);
    } else if (clazz == Float.class) {
      return Float.valueOf(value);
    } else if (clazz == Double.class) {
      return Double.valueOf(value);
    } else if (clazz == String.class) {
      return value;
    }
    return value;
  }

  /**
   * Subclasses should implement the produce(Text line, KijiTableContext context) method instead.
   * {@inheritDoc}
   */
  @Override
  public final void produce(LongWritable fileOffset, Text line, KijiTableContext context)
      throws IOException {
    produce(line, context);
  }

  /** @return an unmodifiable collection of the columns for this bulk importer. */
  protected final Collection<KijiColumnName> getDestinationColumns() {
    Set<KijiColumnName> columns = mTableImportDescriptor.getColumnNameSourceMap().keySet();
    return Collections.unmodifiableSet(columns);
  }

  /**
   * Returns the source for the specified column, or null if the specified column is not a
   * destination column for this importer.
   *
   * @param kijiColumnName the requested Kiji column
   * @return the source for the requested column
   */
  protected final String getSource(KijiColumnName kijiColumnName) {
    return mTableImportDescriptor.getColumnNameSourceMap().get(kijiColumnName);
  }

  /** @return the source for the EntityId. */
  protected final String getEntityIdSource() {
    return mTableImportDescriptor.getEntityIdSource();
  }

  /** @return whether to override the timestamp from system time. */
  protected final boolean isOverrideTimestamp() {
    return null != mTableImportDescriptor.getOverrideTimestampSource();
  }

  /** @return the source for timestamp. */
  protected final String getTimestampSource() {
    return mTableImportDescriptor.getOverrideTimestampSource();
  }

  /**
   * Subclasses should implement the {@link #cleanupImporter(KijiTableContext)} method instead.
   * {@inheritDoc}
   */
  @Override
  public final void cleanup(KijiTableContext context) throws IOException {
    cleanupImporter(context);
  }

  /**
   * Extensible version of {@link KijiBulkImporter#cleanup} for subclasses of
   * DescribedInputTextBulkImporter.
   * Does nothing by default.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   * @throws IOException on I/O error.
   */
  public void cleanupImporter(KijiTableContext context) throws IOException {}

  /**
   * Sets the log rate - the number of lines between log statements for incomplete/rejected lines.
   *
   * @param logRateString The logging rate as a string.
   */
  @HadoopConf(key=CONF_LOG_RATE, usage="The number of lines to skip between log statements")
  protected void setLogRate(String logRateString) {
    if (logRateString != null) {
      try {
        Long logRate = Long.parseLong(logRateString);
        mLogRate = logRate;
      } catch (NumberFormatException ne) {
        LOG.warn("Unable to parse log rate: " + logRateString);
      }
    }
  }

  /**
   * Sets the input descriptor.
   *
   * @param inputDescriptorFile The input descriptor path.
   */
  @HadoopConf(key=CONF_FILE, usage="The input descriptor file.")
  protected void setInputDescriptorPath(String inputDescriptorFile) {

    if (null == inputDescriptorFile || inputDescriptorFile.isEmpty()) {
      // Remind the user to specify this path.
      LOG.error("No input-descriptor path specified.");
      throw new RuntimeException("No input descriptor file specified on the Configuration."
          + "  Did you specify the " + CONF_FILE + " variable?");
    }

    Path descriptorPath = new Path(inputDescriptorFile);
    try {
      LOG.info("Parsing input-descriptor file: " + descriptorPath.toString());
      FileSystem fs = descriptorPath.getFileSystem(getConf());
      FSDataInputStream inputStream = fs.open(descriptorPath);
      mTableImportDescriptor =
          KijiTableImportDescriptor.createFromEffectiveJson(inputStream);

    } catch (IOException ioe) {
      LOG.error("Could not read input-descriptor file: " + descriptorPath.toString());
      throw new RuntimeException("Could not read file: " + descriptorPath.toString());
    }
  }
}

