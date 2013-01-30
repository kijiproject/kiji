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

package org.kiji.mapreduce.output;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * Hadoop output format that writes HFiles that can be loaded directly into HBase region servers.
 *
 * <p> Allows writing entries to any HBase family of the target table.
 *     Each reduce task generates a set of files per HBase family.
 *
 * <p> HFile entries must be properly ordered. This is achieved through the shuffle/sort phase
 *     of the M/R job, combined with an identity reducer.
 *
 * <p> Entries should be partitioned into chunks that fit within an existing region of the target
 *     HTable.
 *
 * <p> The generated HFiles can be loaded into the target HTable with the {@link HFileLoader}.
 */
@ApiAudience.Framework
public final class KijiHFileOutputFormat
    extends FileOutputFormat<HFileKeyValue, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiHFileOutputFormat.class);

  public static final String OUTPUT_EXTENSION = ".hfile";

  public static final String CONF_HREGION_MAX_FILESIZE = "hbase.hregion.max.filesize";
  public static final long DEFAULT_HREGION_MAX_FILESIZE = 256L * 1024L * 1024L;

  public static final String CONF_HFILE_BLOCKSIZE = "hbase.mapreduce.hfileoutputformat.blocksize";
  public static final int DEFAULT_HFILE_BLOCKSIZE = 64 * 1024;

  public static final String CONF_LATEST_TIMESTAMP = "kiji.hfile.latest.timestamp";

  /** {@inheritDoc} */
  @Override
  public RecordWriter<HFileKeyValue, NullWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    return new TableRecordWriter(this, context);
  }

  /**
   * Record writer processing all puts targeted at an entire table.
   *
   * Dispatches records to the appropriate family record writer.
   */
  private static class TableRecordWriter
      extends RecordWriter<HFileKeyValue, NullWritable> {

    // ---------------------------------------------------------------------------------------------

    /**
     * Writes KeyValue records to HFiles.
     *
     * <p>The MapReduce framework will call this writer once for each record that has been
     * emitted from a reducer.  It is assumed that these records will be written in sorted
     * order based on the key (row, family, qualifier, timestamp) of the KeyValue.</p>
     *
     * <p>We will attempt to write these sorted KeyValue objects sequentially into HFiles no
     * larger than <code>maxFileSizeBytes</code>.  However, we will not split a row across
     * multiple HFiles, so exceeding <code>maxFileSizeBytes</code> might be required.</p>
     *
     * <p>Any KeyValue objects that did not have a timestamp specified by the client will be
     * assigned a write time of <code>currentTimestamp</code>.</p>
     */
    private class LocalityGroupRecordWriter
        extends RecordWriter<HFileKeyValue, NullWritable> {

      /** Layout of the locality group this writer writes to. */
      private final LocalityGroupLayout mLGLayout;

      /** HBase family name. */
      private final String mFamily;

      /** Directory path where to write HFiles. */
      private final Path mFamilyDir;

      /** Maximum HFile size, in bytes (best-effort). */
      private final long mMaxFileSizeBytes;

      /** HFile block size, in bytes. */
      private final int mBlockSizeBytes;

      /** The compression algorithm to use for the HFiles. */
      private final Compression.Algorithm mCompressionType;

      /** The HFile writer we currently have open to write KeyValues to. */
      private HFile.Writer mWriter;

      /** The current size of the HFile <code>mCurrentWriter</code> is writing to. */
      private long mCurrentHFileSize = 0;

      /** Key of the last written row. */
      private byte[] mCurrentRow = null;

      /** Counter for HFile file names. */
      private int mHFileCounter = 0;

      /**
       * Constructor.
       *
       * @param context Task attempt context.
       * @param lgLayout Layout of the locality group.
       * @throws IOException on I/O error.
       */
      public LocalityGroupRecordWriter(TaskAttemptContext context, LocalityGroupLayout lgLayout)
          throws IOException {
        mLGLayout = Preconditions.checkNotNull(lgLayout);
        mFamily = lgLayout.getId().toString();

        // These parameters might be specific to each locality group:
        mMaxFileSizeBytes = mConf.getLong(CONF_HREGION_MAX_FILESIZE, DEFAULT_HREGION_MAX_FILESIZE);
        mBlockSizeBytes = mConf.getInt(CONF_HFILE_BLOCKSIZE, DEFAULT_HFILE_BLOCKSIZE);

        mFamilyDir = new Path(mOutputDir, mFamily);
        if (!mFileSystem.exists(mFamilyDir)) {
          if (!mFileSystem.mkdirs(mFamilyDir)) {
            throw new IOException(String.format(
                "Unable to create output directory: %s", mFamilyDir));
          }
        }

        mCompressionType =
            Compression.getCompressionAlgorithmByName(
                mLGLayout.getDesc().getCompressionType().toString().toLowerCase(Locale.ROOT));

        mWriter = openNewWriter();
      }

      /** {@inheritDoc} */
      @Override
      public void write(HFileKeyValue entry, NullWritable unused)
          throws IOException {

        final KeyValue kv = entry.getKeyValue();
        kv.updateLatestStamp(mLatestTimestampBytes);

        final long recordLength = kv.getLength();
        if (mCurrentHFileSize + recordLength >= mMaxFileSizeBytes) {
          // We can't fit this record in the current HFile without exceeding the max file size.

          if (Arrays.equals(mCurrentRow, kv.getRow())) {
            // But we're still adding data for a single row, so we can't close this HFile yet.
            LOG.debug("Reached max HFile size, but waiting to finish this row before closing.");
          } else {
            // Close it and open a new one.
            closeWriter(mWriter);
            mWriter = openNewWriter();
          }
        }

        mWriter.append(kv);
        mCurrentHFileSize += recordLength;

        // Remember the row so we know when we are transitioning.
        mCurrentRow = kv.getRow();
      }

      /** {@inheritDoc} */
      @Override
      public void close(TaskAttemptContext context) throws IOException {
        if (null != mWriter) {
          closeWriter(mWriter);
        }
      }

      /**
       * Opens a new HFile writer for the current column family.
       *
       * @return A new HFile writer.
       * @throws IOException If there is an error.
       */
      private HFile.Writer openNewWriter() throws IOException {
        // Create a unique file for the locality group:
        final Path familyDirectory = new Path(mOutputDir, mFamily);
        if (!mFileSystem.exists(familyDirectory)) {
          mFileSystem.mkdirs(familyDirectory);
        }
        final Path hfilePath = new Path(familyDirectory, String.format("%05d", mHFileCounter));
        mHFileCounter += 1;

        // Create the writer.
        LOG.info("Opening HFile.Writer for family " + mFamily + " at " + hfilePath);
        final HFile.Writer hfileWriter =
            HFile.getWriterFactory(mConf, new CacheConfig(mConf)).createWriter(
                mFileSystem, hfilePath, mBlockSizeBytes, mCompressionType, KeyValue.KEY_COMPARATOR);

        // Reset the current file size.
        mCurrentHFileSize = 0L;
        return hfileWriter;
      }

      /**
       * Closes an HFile writer.
       *
       * @param hfileWriter The writer to close.
       * @throws IOException If there is an error.
       */
      private void closeWriter(HFile.Writer hfileWriter) throws IOException {
        LOG.info("Closing HFile " + hfileWriter.getPath());

        // Write file metadata:
        hfileWriter.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, toBytes(mLatestTimestamp));

        final String taskAttemptID = mContext.getTaskAttemptID().toString();
        hfileWriter.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, toBytes(taskAttemptID));
        hfileWriter.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, toBytes(true));

        hfileWriter.close();
      }
    }

    // ---------------------------------------------------------------------------------------------

    /** Context of the task. */
    private final TaskAttemptContext mContext;

    /** Task configuration. */
    private final Configuration mConf;

    /** Map from locality group column ID to locality group record writer. */
    private final Map<ColumnId, LocalityGroupRecordWriter> mLGWriter = Maps.newHashMap();

    /** Actual timestamp to substitute HConstants.LATEST_TIMESTAMP with. */
    private final long mLatestTimestamp;
    private final byte[] mLatestTimestampBytes;

    /** URI of the output table. */
    private final KijiURI mTableURI;

    /** Layout of the output table. */
    private final KijiTableLayout mLayout;

    private final FileSystem mFileSystem;
    private final Path mOutputDir;

    /**
     * Initializes a new table-wide record writer.
     *
     * @param oformat KijiHFileOutputFormat this writer is built from.
     * @param context Context of the task.
     * @throws IOException on I/O error.
     */
    public TableRecordWriter(KijiHFileOutputFormat oformat, TaskAttemptContext context)
        throws IOException {
      mContext = Preconditions.checkNotNull(context);
      mConf = mContext.getConfiguration();
      mLatestTimestamp = mConf.getLong(CONF_LATEST_TIMESTAMP, System.currentTimeMillis());
      mLatestTimestampBytes = toBytes(mLatestTimestamp);

      mOutputDir = oformat.getDefaultWorkFile(mContext, OUTPUT_EXTENSION);
      mFileSystem = mOutputDir.getFileSystem(mConf);

      mTableURI = KijiURI.newBuilder(mConf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)).build();

      final Kiji kiji = Kiji.Factory.open(mTableURI);
      final KijiTable table = kiji.openTable(mTableURI.getTable());
      mLayout = table.getLayout();
      table.close();
      kiji.release();
    }

    /** {@inheritDoc} */
    @Override
    public void write(HFileKeyValue entry, NullWritable unused)
        throws IOException {
      final ColumnId lgId = ColumnId.fromByteArray(entry.getFamily());
      getWriter(lgId).write(entry, unused);
    }

    /**
     * Gets the record writer for a given locality group.
     *
     * @param lgId Locality group ID.
     * @return the record writer for the specified locality group.
     * @throws IOException on I/O error.
     */
    private synchronized LocalityGroupRecordWriter getWriter(ColumnId lgId)
        throws IOException {

      final LocalityGroupRecordWriter writer = mLGWriter.get(lgId);
      if (writer != null) {
        return writer;
      }

      final String lgName = mLayout.getLocalityGroupIdNameMap().get(lgId);
      Preconditions.checkArgument(lgName != null, String.format(
          "Locality group ID '%s' does not exist in table '%s'.", lgId, mTableURI));
      final LocalityGroupLayout lgroup = mLayout.getLocalityGroupMap().get(lgName);
      final LocalityGroupRecordWriter newWriter = new LocalityGroupRecordWriter(mContext, lgroup);
      mLGWriter.put(lgId, newWriter);
      return newWriter;
    }

    /** {@inheritDoc} */
    @Override
    public void close(TaskAttemptContext context)
        throws IOException {
      for (LocalityGroupRecordWriter writer : mLGWriter.values()) {
        writer.close(context);
      }
    }
  }
}
