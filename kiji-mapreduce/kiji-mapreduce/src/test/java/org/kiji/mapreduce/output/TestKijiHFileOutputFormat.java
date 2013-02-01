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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.HFileKeyValue;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiInstaller;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ColumnId;

/** Tests for KijiHFileOutputFormat. */
public class TestKijiHFileOutputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiHFileOutputFormat.class);

  /** Counter for fake instance IDs. */
  private static final AtomicLong FAKE_INSTANCE_COUNTER = new AtomicLong(0);

  /** NullWritable shortcut. */
  private static final NullWritable NW = NullWritable.get();

  /**
   * Makes a dummy byte array.
   *
   * @param value Byte value to repeat.
   * @param nbytes Number of bytes.
   * @return a byte array with the specified number of bytes and the specified byte value.
   */
  private static byte[] makeBytes(int value, int nbytes) {
    final byte[] bytes = new byte[nbytes];
    for (int i = 0; i < nbytes; ++i) {
      bytes[i] = (byte) value;
    }
    return bytes;
  }

  /**
   * Makes an HFile entry (KeyValue writable-comparable).
   *
   * @param row Row key.
   * @param family HBase family (as a Kiji locality group column ID).
   * @param qualifier HBase qualifier.
   * @param timestamp Cell timestamp.
   * @param value Cell content bytes.
   * @return a new HFileKeyValue with the specified parameters.
   */
  private static HFileKeyValue entry(
      String row, ColumnId family, String qualifier, long timestamp, byte[] value) {
    return new HFileKeyValue(
        toBytes(row), family.toByteArray(), toBytes(qualifier), timestamp, value);
  }

  /**
   * Loads an HFile content into a list of KeyValue entries.
   *
   * @param path Path of the HFile to load.
   * @param conf Configuration.
   * @return the content of the specified HFile, as an ordered list of KeyValue entries.
   * @throws IOException on I/O error.
   */
  private static List<KeyValue> loadHFile(Path path, Configuration conf) throws IOException {
    final FileSystem fs = path.getFileSystem(conf);
    final CacheConfig cacheConf = new CacheConfig(conf);
    final HFile.Reader reader = HFile.createReader(fs, path, cacheConf);
    final HFileScanner scanner = reader.getScanner(false, false);
    final List<KeyValue> kvs = Lists.newArrayListWithCapacity((int) reader.getEntries());
    boolean hasNext = scanner.seekTo();
    while (hasNext) {
      kvs.add(scanner.getKeyValue());
      hasNext = scanner.next();
    }
    reader.close();
    return kvs;
  }

  /**
   * Asserts the content of an HFile.
   *
   * @param path Path of the HFile to validate the content of.
   * @param values Expected KeyValue entries, in order.
   * @throws IOException on I/O error.
   */
  private void assertHFileContent(Path path, KeyValue... values) throws IOException {
    final FileSystem fs = path.getFileSystem(mConf);
    assertTrue(String.format("HFile '%s' does not exist.", path), fs.exists(path));
    final List<KeyValue> kvs = loadHFile(path, mConf);
    assertEquals(kvs.size(), values.length);
    for (int i = 0; i < values.length; ++i) {
      assertEquals(kvs.get(i), values[i]);
    }
  }

  private Configuration mConf;
  private KijiURI mTableURI;
  private Kiji mKiji;
  private File mTempDir;
  private KijiTableLayout mLayout;
  private KijiHFileOutputFormat mFormat;

  private ColumnId mDefaultLGId;
  private ColumnId mInMemoryLGId;

  @Before
  public void setUp() throws Exception {
    mConf = HBaseConfiguration.create();
    mTableURI = KijiURI.newBuilder(String.format(
        "kiji://.fake.%s/default/user", FAKE_INSTANCE_COUNTER.getAndIncrement())).build();

    mTempDir = File.createTempFile("test-" + System.currentTimeMillis() + "-", "");
    Preconditions.checkState(mTempDir.delete());
    Preconditions.checkState(mTempDir.mkdir());

    mConf.set("fs.defaultFS", "file://" + mTempDir.toString());
    mConf.set("mapred.output.dir", "file://" + mTempDir.toString());

    KijiInstaller.get().install(mTableURI, mConf);
    mKiji = Kiji.Factory.open(mTableURI);

    mLayout = new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.FULL_FEATURED), null);
    mKiji.createTable("user", mLayout);

    mDefaultLGId = mLayout.getLocalityGroupMap().get("default").getId();
    mInMemoryLGId = mLayout.getLocalityGroupMap().get("inMemory").getId();

    mConf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, mTableURI.toString());

    mFormat = new KijiHFileOutputFormat();
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(mTempDir);
  }

  @Test
  public void testMaxHFileSizeSameRow() throws Exception {
    final HFileKeyValue entry1 = entry("row-key", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    final HFileKeyValue entry2 = entry("row-key", mDefaultLGId, "b", 1L, makeBytes(0, 1024));

    mConf.setInt(KijiHFileOutputFormat.CONF_HREGION_MAX_FILESIZE, entry1.getLength() + 1);

    final TaskAttemptID taskAttemptId =
        new TaskAttemptID("jobTracker:jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = new TaskAttemptContextImpl(mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, KijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);
    writer.write(entry1, NW);
    writer.write(entry2, NW);
    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    final Path inMemoryDir = new Path(outputDir, mInMemoryLGId.toString());
    assertTrue(!fs.exists(inMemoryDir));

    assertHFileContent(new Path(defaultDir, "00000"), entry1.getKeyValue(), entry2.getKeyValue());
    assertFalse(fs.exists(new Path(defaultDir, "00001")));

    mFormat.getOutputCommitter(context).commitTask(context);
  }

  @Test
  public void testMaxHFileSizeNewRow() throws Exception {
    final HFileKeyValue entry1 = entry("row-key1", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    final HFileKeyValue entry2 = entry("row-key2", mDefaultLGId, "b", 1L, makeBytes(0, 1024));

    mConf.setInt(KijiHFileOutputFormat.CONF_HREGION_MAX_FILESIZE, entry1.getLength() + 1);

    final TaskAttemptID taskAttemptId =
        new TaskAttemptID("jobTracker:jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = new TaskAttemptContextImpl(mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, KijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);
    writer.write(entry1, NW);
    writer.write(entry2, NW);
    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    final Path inMemoryDir = new Path(outputDir, mInMemoryLGId.toString());
    assertFalse(fs.exists(inMemoryDir));

    assertHFileContent(new Path(defaultDir, "00000"), entry1.getKeyValue());
    assertHFileContent(new Path(defaultDir, "00001"), entry2.getKeyValue());
    assertFalse(fs.exists(new Path(defaultDir, "00002")));

    mFormat.getOutputCommitter(context).commitTask(context);
  }

  @Test
  public void testMultipleLayouts() throws Exception {
    final TaskAttemptID taskAttemptId =
        new TaskAttemptID("jobTracker:jtPort", 314, TaskType.MAP, 159, 2);
    final TaskAttemptContext context = new TaskAttemptContextImpl(mConf, taskAttemptId);
    final Path outputDir =
        mFormat.getDefaultWorkFile(context, KijiHFileOutputFormat.OUTPUT_EXTENSION);
    final FileSystem fs = outputDir.getFileSystem(mConf);

    final RecordWriter<HFileKeyValue, NullWritable> writer = mFormat.getRecordWriter(context);

    final HFileKeyValue defaultEntry =
        entry("row-key", mDefaultLGId, "a", 1L, makeBytes(0, 1024));
    writer.write(defaultEntry, NW);
    final HFileKeyValue inMemoryEntry =
        entry("row-key", mInMemoryLGId, "a", 1L, makeBytes(2, 1024));
    writer.write(inMemoryEntry, NW);

    try {
      // Test with an invalid locality group ID:
      final ColumnId invalid = new ColumnId(1234);
      assertTrue(!mLayout.getLocalityGroupIdNameMap().containsKey(invalid));
      writer.write(entry("row-key", invalid, "a", 1L, HConstants.EMPTY_BYTE_ARRAY), NW);
      fail("Output format did not fail on unknown locality group IDs.");
    } catch (IllegalArgumentException iae) {
      LOG.info("Expected error: " + iae);
    }

    writer.close(context);

    final Path defaultDir = new Path(outputDir, mDefaultLGId.toString());
    assertTrue(fs.exists(defaultDir));

    final Path inMemoryDir = new Path(outputDir, mInMemoryLGId.toString());
    assertTrue(fs.exists(inMemoryDir));

    assertHFileContent(new Path(defaultDir, "00000"), defaultEntry.getKeyValue());
    assertHFileContent(new Path(inMemoryDir, "00000"), inMemoryEntry.getKeyValue());

    mFormat.getOutputCommitter(context).commitTask(context);
  }
}
