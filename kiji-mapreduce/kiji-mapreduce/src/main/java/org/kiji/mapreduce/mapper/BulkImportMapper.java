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

package org.kiji.mapreduce.mapper;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.AvroKeyReader;
import org.kiji.mapreduce.AvroValueReader;
import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.HTableReader;
import org.kiji.mapreduce.JobHistoryCounters;
import org.kiji.mapreduce.KijiBulkImporter;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.context.DirectKijiTableWriterContext;
import org.kiji.mapreduce.util.KijiBulkImporters;

/**
 * A mapper that runs a KijiBulkImporter instance inside its map phase.
 *
 * <p>The input to this mapper depends on the format of the data being imported.  For
 * example, if you were importing data from text files using a TextInputFormat, the
 * key would be a LongWritable byte file offset, and the value would be a Text containing
 * a line from the file.</p>
 *
 * <p>The task of generating ProducedWrites that represent data to be imported into a Kiji table
 * is delegated to a KijiBulkImporter subclass whose classname is set in the
 * Configuration.  This mapper will forward each input record to the KijiBulkImporter to
 * allow it to produce the ProducedWrites.  The obtained ProducedWrites will be used to
 * generate an appropriate HBase Put object (output as the map output value).  The map
 * output key is the row of the HTable the Put should be applied to.</p>
 *
 * <p>When paired with a PutSortReducer and an HFileOutputFormat, HFiles will be generated
 * that are ready to be loaded directly into the HBase HTables used for the backing store
 * of a Kiji table.  When run as a map-only job with an HBase TableOutputFormat, the Puts
 * will be sent directly to the HTable for committing.</p>
 *
 * <h1>Usage:</h1>
 * <p><code>setConf()</code> must be called immediately after instantiation.</p>
 *
 * <h1>Configuration:</h1>
 * <ul>
 *   <li><tt>kiji.bulk.import.output.column</tt> (required)
 *       - Specifies the family or fully qualified column to write to.</li>
 *   <li><tt>kiji.bulk.import.enable.dry.run</tt> (optional)
 *       - If set to "true", no writes are generated from this job.
 *       Otherwise, this job completes normally (logging and incrementing counters).
 *       This is useful for verifying that data will import as expected.</li>
 * </ul>
 *
 * @param <INKEY> The type of the MapReduce input key, which depends on the input format.
 * @param <INVALUE> The type of the MapReduce input value, which depends on the input format.
 * @param <OUTKEY> The type of the MapReduce output key, which depends on the output format.
 * @param <OUTVALUE> The type of the MapReduce output value, which depends on the output format.
 */
@ApiAudience.Private
public class BulkImportMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>
    extends Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>
    implements Configurable, AvroKeyReader, AvroValueReader, HTableReader, KijiMapper {

  private static final Logger LOG = LoggerFactory.getLogger(BulkImportMapper.class);

  /** Configuration variable that stores whether this should be executed as a dry run. */
  public static final String CONF_DRY_RUN_ENABLED = "kiji.bulk.import.enable.dry.run";

  /** The job configuration. */
  private Configuration mConf;

  /** The KijiBulkImporter instance to delegate the import work to. */
  private KijiBulkImporter<INKEY, INVALUE> mBulkImporter;

  /** Kiji context for bulk-importers. */
  private KijiTableContext mTableContext;

  /** True if this should be a dry-run where no data is imported to kiji.  Set by setConf(). */
  private boolean mDryRunEnabled = false;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
    mDryRunEnabled = conf.getBoolean(CONF_DRY_RUN_ENABLED, false);
    if (mDryRunEnabled) {
      LOG.info("Dry-run enabled.  No data will be written as a result of this job.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException {
    try {
      super.setup(context);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
    final Configuration conf = context.getConfiguration();

    mBulkImporter = KijiBulkImporters.create(conf);
    mTableContext = new DirectKijiTableWriterContext(context);

    mBulkImporter.setup(mTableContext);
  }

  @Override
  protected void map(INKEY key, INVALUE value, Context mapContext)
      throws IOException {
    mBulkImporter.produce(key, value, mTableContext);
    mapContext.getCounter(JobHistoryCounters.BULKIMPORTER_RECORDS_PROCESSED).increment(1);
  }

  /**
   * Returns whether this bulk import should be executed as a dry-run.
   * If true, no writes will be produced, although logging and counters will
   * operate as usual.  If false (default), writes will be generated as expected.
   *
   * @return Whether this bulk import should be executed as a dry run.
   */
  protected boolean isDryRunEnabled() {
    return mDryRunEnabled;
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup(Context context) throws IOException {
    Preconditions.checkNotNull(mTableContext);
    mBulkImporter.cleanup(mTableContext);
    mTableContext = null;
    try {
      super.cleanup(context);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    KijiBulkImporter<INKEY, INVALUE> bulkImporter = KijiBulkImporters.create(getConf());
    if (bulkImporter instanceof AvroKeyReader) {
      LOG.debug("Bulk importer " + bulkImporter.getClass().getName()
          + " implements AvroKeyReader, querying for reader schema.");
      return ((AvroKeyReader) bulkImporter).getAvroKeyReaderSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    KijiBulkImporter<INKEY, INVALUE> bulkImporter = KijiBulkImporters.create(getConf());
    if (bulkImporter instanceof AvroValueReader) {
      LOG.debug("Bulk importer " + bulkImporter.getClass().getName()
          + " implements AvroValueReader, querying for reader schema.");
      return ((AvroValueReader) bulkImporter).getAvroValueReaderSchema();
    }
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Scan getInputHTableScan(Configuration conf) throws IOException {
    KijiBulkImporter<INKEY, INVALUE> bulkImporter = KijiBulkImporters.create(conf);
    if (bulkImporter instanceof HTableReader) {
      LOG.debug("Bulk importer " + bulkImporter.getClass().getName()
          + " implements HTableReader, querying for input HTable Scan specification.");
      return ((HTableReader) bulkImporter).getInputHTableScan(conf);
    }
    // The wrapped bulk importer doesn't need to read from an HTable, so return null to
    // tell the caller that we also don't need to read from an HTable.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return HFileKeyValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}
