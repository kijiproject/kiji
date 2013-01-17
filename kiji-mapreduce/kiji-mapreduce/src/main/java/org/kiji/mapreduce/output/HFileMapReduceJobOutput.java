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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.hadoopbackport.TotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.context.HFileWriterContext;
import org.kiji.schema.KijiRowKeySplitter;
import org.kiji.schema.KijiTable;
import org.kiji.schema.impl.HBaseKijiTable;

/**
 * MapReduce output configuration for Kiji jobs that generate HFiles.
 *
 * <p>The generated HFiles can be directly loaded into the regions of an existing HTable.
 * Use a {@link org.kiji.mapreduce.HFileLoader} to load HFiles into a Kiji table.</p>
 */
@ApiAudience.Public
public class HFileMapReduceJobOutput extends KijiTableMapReduceJobOutput {
  private static final Logger LOG = LoggerFactory.getLogger(HFileMapReduceJobOutput.class);

  /**
   * If <code>mNumSplits</code> has this special value, the number of splits should be
   * set equal to the number of existing regions in the target Kiji table.
   */
  private static final int NUM_SPLITS_AUTO = 0;

  /** The path to the directory to create the HFiles in. */
  private final Path mPath;

  /**
   * The number of splits for the output.  May be zero, which means match the number of
   * splits to be the number of existing regions in the table.
   */
  private final int mNumSplits;

  /**
   * Create job output of HFiles that can be efficiently loaded into a Kiji table.
   * The number of HFiles created (which determines the number of reduce tasks) will
   * match the number of existing regions in the target Kiji table.
   *
   * @param table The kiji table the resulting HFiles are intended for.
   * @param path The directory path to output the HFiles to.
   */
  public HFileMapReduceJobOutput(KijiTable table, Path path) {
    this(table, path, NUM_SPLITS_AUTO);
  }

  /**
   * Create job output of HFiles that can be efficiently loaded into a Kiji table.
   * The number of HFiles created (which determines the number of reduce tasks) is
   * specified with the <code>numSplits</code> argument.  Controlling the number of
   * splits is only possible when targeting a Kiji table with &lt;hashRowKeys&gt;
   * enabled.  Typically, you should allow the system to
   * match the number of splits to the number of regions in the table by using the {@link
   * #HFileMapReduceJobOutput(KijiTable, Path)} constructor instead.
   *
   * @param table The kiji table the resulting HFiles are intended for.
   * @param path The directory path to output the HFiles to.
   * @param numSplits The number of splits (determines the number of reduce tasks).
   */
  public HFileMapReduceJobOutput(KijiTable table, Path path, int numSplits) {
    super(table);
    mPath = path;
    mNumSplits = numSplits;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    super.configure(job);
    final Configuration conf = job.getConfiguration();

    // Set the output path.
    FileOutputFormat.setOutputPath(job, mPath);

    // Hadoop output format:
    job.setOutputFormatClass(KijiHFileOutputFormat.class);

    // Kiji table context:
    conf.setClass(
        KijiConfKeys.KIJI_TABLE_CONTEXT_CLASS,
        HFileWriterContext.class,
        KijiTableContext.class);

    // Configure the total order partitioner so generated HFile shards are contiguous and sorted.
    final HBaseKijiTable kijiTable = HBaseKijiTable.downcast(getTable());
    if (NUM_SPLITS_AUTO == mNumSplits) {
      configurePartitioner(job, kijiTable.getHTable(), getRegionStartKeys(kijiTable.getHTable()));
    } else {
      switch (kijiTable.getLayout().getDesc().getKeysFormat().getEncoding()) {
      case RAW: {
        // The user has explicitly specified how many HFiles to create, but this is not
        // possible when row key hashing is disabled.
        throw new JobConfigurationException(String.format(
            "Table '%s' has row key hashing disabled, so the number of HFile splits must be"
            + "determined by the number of HRegions in the HTable. "
            + "Use an HFileMapReduceJobOutput constructor that enables auto splitting.",
            kijiTable.getName()));
      }
      case HASH:
      case HASH_PREFIX: {
        // Those cases are supported:
        break;
      }
      default:
        throw new RuntimeException("Unhandled row key encoding: "
            + kijiTable.getLayout().getDesc().getKeysFormat().getEncoding());
      }
      configurePartitioner(job, kijiTable.getHTable(), generateEvenStartKeys(mNumSplits));
    }

    // Adds HBase dependency jars to the distributed cache so they appear on the task classpath:
    GenericTableMapReduceUtil.addAllDependencyJars(job);
  }

  /**
   * Configures the partitioner for generating HFiles.
   *
   * <p>Each generated HFile should fit within a region of of the target table.
   * Additionally, it's optimal to have only one HFile to load into each region, since a
   * read from that region will require reading from each HFile under management (until
   * compaction happens and merges them all back into one HFile).</p>
   *
   * <p>To achieve this, we configure a TotalOrderPartitioner that will partition the
   * records output from the Mapper based on their rank in a total ordering of the
   * keys.  The <code>startKeys</code> argument should contain a list of the first key in
   * each of those partitions.</p>
   *
   * @param job The job to configure.
   * @param table The target HTable.
   * @param startKeys A list of keys that will mark the boundaries between the partitions
   *     for the sorted map output records.
   * @throws IOException If there is an error.
   */
  private static void configurePartitioner(
      Job job, HTableInterface table, List<HFileKeyValue> startKeys)
      throws IOException {
    job.setPartitionerClass(TotalOrderPartitioner.class);

    LOG.info("Configuring " + startKeys.size() + " reduce partitions.");
    job.setNumReduceTasks(startKeys.size());

    // Write the file that the TotalOrderPartitioner reads to determine where to partition records.
    Path partitionFilePath = new Path(
        job.getWorkingDirectory(), "partitions_" + System.currentTimeMillis());
    LOG.info("Writing partition information to " + partitionFilePath);

    FileSystem fs = partitionFilePath.getFileSystem(job.getConfiguration());
    partitionFilePath = partitionFilePath.makeQualified(fs);
    writePartitionFile(job.getConfiguration(), partitionFilePath, startKeys);

    // Add it to the distributed cache.
    URI cacheUri;
    try {
      cacheUri = new URI(partitionFilePath.toString() + "#" + TotalOrderPartitioner.DEFAULT_PATH);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    DistributedCache.addCacheFile(cacheUri, job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());
  }

  /**
   * <p>Write out a SequenceFile that can be read by TotalOrderPartitioner
   * that contains the split points in startKeys.</p>
   *
   * <p>This method was copied from HFileOutputFormat in hbase-0.90.1-cdh3u0.  I had to
   * copy it because it's private.</p>
   *
   * @param conf The job configuration.
   * @param partitionsPath output path for SequenceFile.
   * @param startKeys the region start keys to use as the partitions.
   * @throws IOException If there is an error.
   */
  private static void writePartitionFile(
      Configuration conf, Path partitionsPath, List<HFileKeyValue> startKeys) throws IOException {
    if (startKeys.isEmpty()) {
      throw new IllegalArgumentException("No regions passed");
    }

    // We're generating a list of split points, and we don't ever
    // have keys < the first region (which has an empty start key)
    // so we need to remove it. Otherwise we would end up with an
    // empty reducer with index 0.
    TreeSet<HFileKeyValue> sorted = new TreeSet<HFileKeyValue>();
    sorted.addAll(startKeys);

    HFileKeyValue first = sorted.first();
    if (0 != first.getRowKey().length) {
      throw new IllegalArgumentException(
          "First region of table should have empty start row key. Instead has: "
          + Bytes.toStringBinary(first.getRowKey()));
    }
    sorted.remove(first);

    // Write the actual file
    FileSystem fs = partitionsPath.getFileSystem(conf);
    SequenceFile.Writer writer = SequenceFile.createWriter(
        fs, conf, partitionsPath, HFileKeyValue.class, NullWritable.class);

    try {
      for (HFileKeyValue startKey : sorted) {
        writer.append(startKey, NullWritable.get());
      }
    } finally {
      writer.close();
    }
  }

  /**
   * <p>Generate a list of start keys (one per region).  Since we know
   * that the row keys in kiji are byte strings of length 16, we can reliably split
   * them evenly.</p>
   *
   * @param numRegions The number of regions to generate start keys for.
   * @return A list of start keys with size equal to <code>numRegions</code>.
   */
  private static List<HFileKeyValue> generateEvenStartKeys(int numRegions) {
    List<HFileKeyValue> startKeys = new ArrayList<HFileKeyValue>(numRegions);

    // The first key is a special case, it must be empty.
    startKeys.add(HFileKeyValue.createFromRowKey(HConstants.EMPTY_BYTE_ARRAY));

    if (numRegions > 1) {
      byte[][] splitKeys = KijiRowKeySplitter.getSplitKeys(numRegions);
      for (byte[] hbaseRowKey : splitKeys) {
        startKeys.add(HFileKeyValue.createFromRowKey(hbaseRowKey));
      }
    }
    return startKeys;
  }

  /**
   * Return the start keys of all of the regions in this table as a list of KeyValues.
   *
   * <p>This method was copied from HFileOutputFormat of 0.90.1-cdh3u0 and modified to
   * return KeyValue instead of ImmutableBytesWritable.</p>
   *
   * @param table The HTable to get region boundary start keys for.
   * @return The list of start keys.
   * @throws IOException If there is an error.
   */
  private static List<HFileKeyValue> getRegionStartKeys(HTableInterface table) throws IOException {
    final HBaseAdmin admin = new HBaseAdmin(table.getConfiguration());
    try {
      final List<HRegionInfo> regions = admin.getTableRegions(table.getTableName());
      final List<HFileKeyValue> ret = new ArrayList<HFileKeyValue>(regions.size());
      for (HRegionInfo region : regions) {
        ret.add(HFileKeyValue.createFromRowKey(region.getStartKey()));
      }
      return ret;
    } finally {
      admin.close();
    }
  }
}
