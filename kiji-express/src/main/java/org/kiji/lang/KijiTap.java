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

package org.kiji.lang;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.DistributedCacheJars;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.util.Jars;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/**
 * A {@link Tap} for reading data from a Kiji table. The tap is responsible for configuring a
 * MapReduce job with the correct input format for reading from a Kiji table,
 * as well as the proper classpath dependencies for MapReduce tasks.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiTap is serialized,
 * the result is not persisted anywhere making serialVersionUID unnecessary.
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class KijiTap
    extends Tap<JobConf, RecordReader, OutputCollector> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTap.class);

  /** The URI of the table to be read through this tap. */
  private final String mTableURI;
  /** The scheme to be used with this tap. */
  private final KijiScheme mScheme;
  /** A unique identifier for this tap instance. */
  private final String mId;

  /**
   * Creates a new instance of this tap.
   *
   * @param tableURI for the Kiji table this tap will be used to read.
   * @param scheme to be used with this tap that will convert data read from Kiji into Cascading's
   *     tuple model.
   * @throws IOException if there is an error creating the tap.
   */
  public KijiTap(KijiURI tableURI, KijiScheme scheme) throws IOException {
    // Set the scheme for this tap.
    super(scheme);
    mTableURI = tableURI.toString();
    mScheme = scheme;
    mId = UUID.randomUUID().toString();
  }

  /** {@inheritDoc} */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // Configure the job's input format.
    conf.setInputFormat(KijiInputFormat.class);

    // Store the input table.
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, mTableURI);

    // Put Kiji dependency jars on the distributed cache.
    try {
      getStepConfigDef().setProperty(
          "tmpjars",
          StringUtils.join(findKijiJars(conf), ","));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    super.sourceConfInit(process, conf);
  }

  /** {@inheritDoc} */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // TODO(CHOP-35): Use an output format that writes to HFiles.
    // Configure the job's output format.
    conf.setOutputFormat(NullOutputFormat.class);

    // Store the output table.
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, mTableURI);

    // Put Kiji dependency jars on the distributed cache.
    try {
      getStepConfigDef().setProperty(
          "tmpjars",
          StringUtils.join(findKijiJars(conf), ","));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    super.sinkConfInit(process, conf);
  }

  /** {@inheritDoc} */
  @Override
  public String getIdentifier() {
    return mId;
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryIterator openForRead(
      FlowProcess<JobConf> jobConfFlowProcess,
      RecordReader recordReader) throws IOException {
    return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryCollector openForWrite(
      FlowProcess<JobConf> jobConfFlowProcess,
      OutputCollector outputCollector) throws IOException {
    return new HadoopTupleEntrySchemeCollector(jobConfFlowProcess, this, outputCollector);
  }

  /** {@inheritDoc} */
  @Override
  public boolean createResource(JobConf jobConf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteResource(JobConf jobConf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean resourceExists(JobConf jobConf) throws IOException {
    final KijiURI uri = KijiURI.newBuilder(mTableURI).build();
    final String tableName = uri.getTable();
    final Kiji kiji = Kiji.Factory.open(uri);

    return kiji.getTableNames().contains(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public long getModifiedTime(JobConf jobConf) throws IOException {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof KijiTap)) {
      return false;
    }

    final KijiTap tap = (KijiTap) other;
    return Objects.equal(mTableURI, tap.mTableURI)
        && Objects.equal(mScheme, tap.mScheme)
        && Objects.equal(mId, tap.mId);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mTableURI, mScheme, mId);
  }

  /**
   * Finds Kiji dependency jars and returns a list of their paths. Use this method to find
   * jars that need to be sent to Hadoop's DistributedCache.
   *
   * @param fsConf Configuration containing options for the filesystem containing jars.
   * @throws IOException If there is an error.
   * @return A list of paths to dependency jars.
   */
  private static List<String> findKijiJars(Configuration fsConf) throws IOException {
    final List<String> jars = Lists.newArrayList();

    // Find the kiji jars.
    File schemaJar;
    File mapreduceJar;
    File chopsticksJar;
    try {
      schemaJar = new File(Jars.getJarPathForClass(Kiji.class));
      LOG.debug("Found kiji-schema jar: {}", schemaJar);

      mapreduceJar = new File(Jars.getJarPathForClass(DistributedCacheJars.class));
      LOG.debug("Found kiji-mapreduce jar: {}", mapreduceJar);

      chopsticksJar = new File(Jars.getJarPathForClass(KijiTap.class));
      LOG.debug("Found kiji-chopsticks jar: {}", chopsticksJar);
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("The kiji jars could not be found; no kiji dependency jars will be "
          + "loaded onto the distributed cache.");
      return jars;
    }

    // Add kiji-schema dependencies.
    jars.addAll(DistributedCacheJars.getJarsFromDirectory(fsConf, schemaJar.getParentFile()));

    // Add kiji-mapreduce dependencies.
    jars.addAll(DistributedCacheJars.getJarsFromDirectory(fsConf, mapreduceJar.getParentFile()));

    // Add kiji-chopsticks dependencies.
    Preconditions.checkState(
        chopsticksJar.getName().endsWith(".jar"),
        "Failed to find kiji-chopsticks jar. Instead found: {}", chopsticksJar.getName());
    final FileSystem fs = FileSystem.getLocal(fsConf);
    jars.add(new Path(chopsticksJar.getCanonicalPath()).makeQualified(fs).toString());
    fs.close();

    // Remove duplicate jars and return.
    return DistributedCacheJars.deDuplicateJarNames(jars);
  }
}
