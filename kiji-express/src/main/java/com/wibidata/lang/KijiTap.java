package com.wibidata.lang;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.kiji.mapreduce.DistributedCacheJars;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.util.Jars;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Must be used with KijiScheme.
 */
@SuppressWarnings("rawtypes")
public class KijiTap
    extends Tap<JobConf, RecordReader, OutputCollector> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiTap.class);
  private static final long serialVersionUID = 1L;

  private final String mTableURI;
  private final KijiScheme mScheme;
  private final String mId = UUID.randomUUID().toString();

  public KijiTap(KijiURI tableURI, KijiScheme scheme) {
    super(scheme);

    mTableURI = tableURI.toString();
    mScheme = scheme;
  }

  /** {@inheritDoc} */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    conf.setInputFormat(KijiInputFormat.class);
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, mTableURI);

    getStepConfigDef().setProperty("poo.poo", "foo");

    try {
      final List<String> allJars = new ArrayList<String>();

      // Get the path to the kiji-schema jar.
      String kijiSchemaJarPath;
      try {
        kijiSchemaJarPath = Jars.getJarPathForClass(Kiji.class);
      } catch (ClassNotFoundException e) {
        LOG.warn("The kiji-schema jar could not be found, so no kiji dependency jars will be "
            + "loaded onto the distributed cache.");
        return;
      }
      LOG.info("Found kiji-schema jar: {}", kijiSchemaJarPath);

      // Get the path to the kiji-mapreduce jar.
      String kijiMRJarPath;
      try {
        kijiMRJarPath = Jars.getJarPathForClass(DistributedCacheJars.class);
      } catch (ClassNotFoundException e) {
        LOG.warn("The kiji-mapreduce jar could not be found, so no kiji dependency jars will be "
            + "loaded onto the distributed cache.");
        return;
      }
      LOG.info("Found kiji-mapreduce jar: {}", kijiMRJarPath);

      // Get the path to the kiji-scalding jar.
      String kijiScaldingPath;
      try {
        kijiScaldingPath = Jars.getJarPathForClass(KijiTap.class);
      } catch (ClassNotFoundException e) {
        LOG.warn("The kiji-scalding jar could not be found.");
        return;
      }
      LOG.info("Found kiji-scalding jar: {}", kijiScaldingPath);

      // Add kiji-schema dependencies.
      final File kijiSchemaLibDir = new File(kijiSchemaJarPath).getParentFile();
      Preconditions.checkState(
          kijiSchemaLibDir.isDirectory(),
          "Got a path to kiji-schema lib that isn't a directory: " + kijiSchemaLibDir.getPath());
      LOG.info("Adding kiji-schema dependency jars to the distributed cache of the job: {}",
          kijiSchemaLibDir.getPath());
      allJars.addAll(DistributedCacheJars.getJarsFromDirectory(conf, kijiSchemaLibDir));

      // Add kiji-mapreduce dependencies.
      final File kijiMRLibDir = new File(kijiMRJarPath).getParentFile();
      Preconditions.checkState(
          kijiMRLibDir.isDirectory(),
          "Got a path to kiji-mapreduce lib that isn't a directory: " + kijiMRLibDir.getPath());
      LOG.info("Adding kiji-mapreduce dependency jars to the distributed cache of the job: {}",
          kijiMRLibDir.getPath());
      allJars.addAll(DistributedCacheJars.getJarsFromDirectory(conf, kijiMRLibDir));

      // Add kiji-scalding dependencies.
      final File kijiScaldingJar = new File(kijiScaldingPath);
      Preconditions.checkNotNull(
          kijiScaldingJar,
          "Can't find the kiji-scalding jar.");
      LOG.info("Adding kiji-scalding jar: {}", kijiScaldingJar.getPath());
      final FileSystem fs = FileSystem.getLocal(conf);
      // TODO: check to ensure that this is actually a jar.
      allJars.add(new Path(kijiScaldingJar.getCanonicalPath()).makeQualified(fs).toString());

      getStepConfigDef().setProperty(
          "tmpjars",
          StringUtils.join(DistributedCacheJars.deDuplicateJarNames(allJars), ","));

      // TODO: Do something better than wrapping the exception.
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    // Store the data request.
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(mScheme.getDataRequest()));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);

    super.sinkConfInit(process, conf);
  }

  /** {@inheritDoc} */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // TODO: Implement
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
      OutputCollector outputCollector) {
    // TODO: Implement.
    throw new UnsupportedOperationException("Writing is not supported with KijiTap.");
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


  // TODO: Implement equals, hashCode.
}
