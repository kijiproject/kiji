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

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.HFileKeyValue;

/**
 * An output format that writes HFiles that can be loaded directly into HBase HRegions.
 *
 * <p>This is better than the existing HBase HFileOutputFormat because it uses the
 * MapReduce framework for sorting the KeyValue objects instead of requiring an in-memory
 * sort to happen in the reduce phase.</p>
 *
 * <p>For this output format to be useful, the KeyValues should already be sorted and
 * partitioned into chunks that fit within an existing region of the target HTable.</p>
 *
 * <p>You may only write HFiles with a single column family at a time.</p>
 */
public class KijiHFileOutputFormat extends FileOutputFormat<HFileKeyValue, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiHFileOutputFormat.class);

  public static final String CONF_HREGION_MAX_FILESIZE = "hbase.hregion.max.filesize";
  public static final long DEFAULT_HREGION_MAX_FILESIZE = 256L * 1024L * 1024L;

  public static final String CONF_HFILE_BLOCKSIZE = "hbase.mapreduce.hfileoutputformat.blocksize";
  public static final int DEFAULT_HFILE_BLOCKSIZE = 64 * 1024;

  public static final String CONF_HFILE_COMPRESSION = "hfile.compression";
  public static final String DEFAULT_HFILE_COMPRESSION = Compression.Algorithm.NONE.getName();

  /** {@inheritDoc} */
  @Override
  public RecordWriter<HFileKeyValue, NullWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    // Downcast: FileOutputFormat always creates a FileOutputCommitter.
    FileOutputCommitter outputCommitter = (FileOutputCommitter) getOutputCommitter(context);

    return new HFileRecordWriter(context,
        outputCommitter.getWorkPath(),
        conf.getLong(CONF_HREGION_MAX_FILESIZE, DEFAULT_HREGION_MAX_FILESIZE),
        conf.getInt(CONF_HFILE_BLOCKSIZE, DEFAULT_HFILE_BLOCKSIZE),
        Compression.getCompressionAlgorithmByName(
            conf.get(CONF_HFILE_COMPRESSION, DEFAULT_HFILE_COMPRESSION)),
        System.currentTimeMillis());
  }

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
  private static class HFileRecordWriter extends RecordWriter<HFileKeyValue, NullWritable> {
    /** The HBase configuration. */
    private final Configuration mConf;
    /** The task attempt using this record writer. */
    private final String mTaskAttemptId;
    /** The output path for the HFiles. */
    private final Path mOutputPath;
    /** The max HFile size in bytes (best-effort). */
    private final long mMaxFileSizeBytes;
    /** The HFile block size in bytes. */
    private final int mBlockSizeBytes;
    /** The compression algorithm to use for the HFiles. */
    private final Compression.Algorithm mCompressionType;
    /** The timestamp to use for KeyValues that didn't explicitly specify a timestamp. */
    private final byte[] mCurrentTimestamp;
    /** The file system the HFiles will live on. */
    private final FileSystem mFileSystem;

    /** The column family we are writing to. */
    private byte[] mFamily;
    /** The HFile writer we currently have open to write KeyValues to. */
    private HFile.Writer mCurrentWriter;
    /** The current size of the HFile <code>mCurrentWriter</code> is writing to. */
    private long mCurrentHFileSize;
    /** The previous row key that was written. */
    private byte[] mPreviousRow;

    /**
     * Constructor.
     *
     * @param context The task attempt context.
     * @param outputPath The output directory for the HFiles.
     * @param maxFileSizeBytes The maximum file size in bytes for the HFiles (best-effort).
     * @param blockSizeBytes The file block size in bytes.
     * @param compressionType The type of compression to use for the HFile.
     * @param currentTimestamp The timestamp to use for KeyValues that didn't explicitly
     *     specify a timestamp.
     * @throws IOException If there is an error.
     */
    public HFileRecordWriter(TaskAttemptContext context, Path outputPath, long maxFileSizeBytes,
        int blockSizeBytes, Compression.Algorithm compressionType, long currentTimestamp)
        throws IOException {
      mConf = context.getConfiguration();
      mTaskAttemptId = context.getTaskAttemptID().toString();
      mOutputPath = outputPath;
      mMaxFileSizeBytes = maxFileSizeBytes;
      mBlockSizeBytes = blockSizeBytes;
      mCompressionType = compressionType;
      mCurrentTimestamp = Bytes.toBytes(currentTimestamp);
      mFileSystem = outputPath.getFileSystem(mConf);

      mFamily = null;
      mCurrentWriter = null;
      mCurrentHFileSize = 0L;
      mPreviousRow = HConstants.EMPTY_BYTE_ARRAY;
    }

    /** {@inheritDoc} */
    @Override
    public void write(HFileKeyValue keyValue, NullWritable ignore)
        throws IOException, InterruptedException {
      if (null == mCurrentWriter) {
        // Initialize the writer -- this is the first call to write().
        mFamily = keyValue.getFamily();
        mCurrentWriter = openNewWriter();
      }

      // Make sure we always write to the same column family.
      if (!Arrays.equals(keyValue.getFamily(), mFamily)) {
        throw new IOException("Only one family may be written to an HFile. "
            + "Found second family " + Bytes.toString(keyValue.getFamily()));
      }

      long recordLength = keyValue.getLength();
      if (mCurrentHFileSize + recordLength >= mMaxFileSizeBytes) {
        // We can't fit this record in the current HFile without exceeding the max file size.

        if (Arrays.equals(mPreviousRow, keyValue.getRowKey())) {
          // But we're still adding data for a single row, so we can't close this HFile yet.
          LOG.debug("Reached max HFile size, but waiting to finish this row before closing.");
        } else {
          // Close it and open a new one.
          closeWriter(mCurrentWriter);
          mCurrentWriter = openNewWriter();
        }
      }

      // Write the KeyValue to the HFile.
      keyValue.updateLatestStamp(mCurrentTimestamp);
      mCurrentWriter.append(keyValue.getKeyValue());
      mCurrentHFileSize += recordLength;

      // Remember the row so we know when we are transitioning.
      mPreviousRow = keyValue.getRowKey();
    }

    /** {@inheritDoc} */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      if (null != mCurrentWriter) {
        closeWriter(mCurrentWriter);
      }
    }

    /**
     * Opens a new HFile writer for the current column family.
     *
     * @return A new HFile writer.
     * @throws IOException If there is an error.
     */
    private HFile.Writer openNewWriter() throws IOException {
      // Create a unique file in the family directory for the file.
      Path familyDirectory = new Path(mOutputPath, Bytes.toString(mFamily));
      if (!mFileSystem.exists(familyDirectory)) {
        mFileSystem.mkdirs(familyDirectory);
      }
      Path hfilePath = StoreFile.getUniqueFile(mFileSystem, familyDirectory);

      // Create the writer.
      LOG.info("Opening HFile.Writer for family " + Bytes.toString(mFamily) + " at " + hfilePath);
      HFile.Writer hfileWriter = HFile.getWriterFactory(mConf, new CacheConfig(mConf))
          .createWriter(mFileSystem, hfilePath, mBlockSizeBytes, mCompressionType,
                        KeyValue.KEY_COMPARATOR);

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

      // Write the file metadata at the end.
      hfileWriter.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
          Bytes.toBytes(System.currentTimeMillis()));
      hfileWriter.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
          Bytes.toBytes(mTaskAttemptId));
      hfileWriter.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
          Bytes.toBytes(true));

      // Close it.
      hfileWriter.close();
    }
  }
}
