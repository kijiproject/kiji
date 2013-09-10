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

package org.kiji.mapreduce.framework;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.DistributedCacheJars;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.impl.HTableReader;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigValidator;
import org.kiji.mapreduce.kvstore.impl.XmlKeyValueStoreParser;
import org.kiji.mapreduce.platform.KijiMRPlatformBridge;
import org.kiji.mapreduce.util.AvroMapReduce;
import org.kiji.mapreduce.util.Jars;
import org.kiji.schema.Kiji;

/**
 * Base class for builders that configure <code>MapReduceJob</code>s.
 *
 * @param <T> The type of the builder class. Users should use a concrete
 * implementation such as {@link org.kiji.mapreduce.gather.KijiGatherJobBuilder}.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public abstract class MapReduceJobBuilder<T extends MapReduceJobBuilder<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceJobBuilder.class);

  /**
   * Name of the system property that may contain the path to a lib/*.jar directory to embed
   * with MapReduce jobs.
   */
  public static final String JOB_LIB_PROPERTY = "org.kiji.mapreduce.job.lib";
  public static final String ADD_CLASSPATH_TO_JOB_DCACHE_PROPERTY =
      "org.kiji.mapreduce.add.classpath.to.job.dcache";

  /** A list of directories containing jars to be loaded onto the distributed cache. */
  private final List<Path> mJarDirectories;

  /** Base Hadoop configuration used to build the MapReduce job. */
  private Configuration mConf;

  /** The output for the job. */
  private MapReduceJobOutput mJobOutput;

  /** Bindings from store names to KeyValueStore implementations. */
  private Map<String, KeyValueStore<?, ?>> mBoundStores;

  /** Creates a new <code>MapReduceJobBuilder</code> instance. */
  protected MapReduceJobBuilder() {
    mJarDirectories = Lists.newArrayList();
    mBoundStores = Maps.newHashMap();

    mJobOutput = null;
  }

  /**
   * Builds a runnable MapReduce job.
   *
   * @return A configured MapReduce job, ready to be run.
   * @throws IOException If there is an error.
   */
  public final KijiMapReduceJob build() throws IOException {
    Preconditions.checkNotNull(mConf, "Must set the job base configuration using .withConf()");
    final Job job = new Job(mConf);
    configureJob(job);
    return build(job);
  }

  /**
   * Adds a local directory of jars to the distributed cache of the job.
   *
   * @param jarDirectory The path to a directory of jar files in the local file system.
   * @return This builder instance so you may chain configuration method calls.
   */
  public T addJarDirectory(File jarDirectory) {
    return addJarDirectory("file:" + jarDirectory.getAbsolutePath());
  }

  /**
   * Adds a directory of jars to the distributed cache of the job.
   *
   * @param jarDirectory The path to a directory of jar files.
   *     Path may be qualified (eg. "hdfs://path/to/dir", or "file:/path/to/dir"),
   *     or unqualified (eg. "path/to/dir"), in which case it will be resovled against the
   *     local file system.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T addJarDirectory(Path jarDirectory) {
    Path jarDirPath = null;
    try {
      jarDirPath = (jarDirectory.toUri().getScheme() == null)
          ? new Path("file:" + jarDirectory)
          : jarDirectory;
    } catch (IllegalArgumentException iae) {
      // A URISyntaxException was thrown by the Path c'tor, wrapped in an
      // IllegalArgumentException.  Meaning the Path is a relative path, not an absolute one,
      // and contains no scheme identifier. Canonicalize the filename and add the "file:"
      // scheme prefix to the canonizalized Path
      jarDirPath = new Path("file:" + new File(jarDirectory.toString()).getAbsolutePath());
    }

    mJarDirectories.add(jarDirPath);
    return (T) this;
  }

  /**
   * Adds a directory of jars to the distributed cache of the job.
   *
   * @param jarDirectory The path to a directory of jar files.
   *     Path may be qualified (eg. "hdfs://path/to/dir", or "file:/path/to/dir"),
   *     or unqualified (eg. "path/to/dir"), in which case it will be resovled against the
   *     local file system.
   * @return This builder instance so you may chain configuration method calls.
   */
  public T addJarDirectory(String jarDirectory) {
    return addJarDirectory(new Path(jarDirectory));
  }

  /**
   * Configures the job output.
   *
   * <p>Classes that override this method should call super.getJobOutput or override
   * {@link MapReduceJobBuilder#getJobOutput() getJobOutput} also.</p>
   *
   * @param jobOutput The output for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withOutput(MapReduceJobOutput jobOutput) {
    mJobOutput = jobOutput;
    return (T) this;
  }

  /**
   * Provides access to the job output that the builder is configured with.
   *
   * <p>Classes should override this method if they also override
   * {@link MapReduceJobBuilder#withOutput(MapReduceJobOutput) withOutput}.</p>
   *
   * @return The output for the job the builder will create.
   */
  protected MapReduceJobOutput getJobOutput() {
    return mJobOutput;
  }

  /**
   * Binds a key-value store name to the specified KeyValueStore implementation.
   *
   * @param storeName the name of the KeyValueStore.
   * @param storeImpl the KeyValueStore implementation to use.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public final T withStore(String storeName, KeyValueStore<?, ?> storeImpl) {
    if (null == storeImpl) {
      throw new RuntimeException("Cannot bind a store to a null implementation");
    }

    if (mBoundStores.containsKey(storeName)) {
      LOG.warn("Replacing existing definition for KeyValueStore " + storeName);
    }

    mBoundStores.put(storeName, storeImpl);
    return (T) this;
  }

  /**
   * Parses the KeyValueStore specification XML file and adds all store bindings in the
   * file to the job.
   *
   * @see #withStoreBindings(InputStream)
   * @param specFilename the name of the XML file specifying the input stores.
   * @return This builder instance so you may chain configuration method calls.
   * @throws IOException if there is an error reading or parsing the spec file.
   */
  public final T withStoreBindingsFile(String specFilename) throws IOException {
    InputStream fis = null;
    try {
      fis = new FileInputStream(new File(specFilename));
      return withStoreBindings(fis);
    } finally {
      if (null != fis) {
        fis.close();
      }
    }
  }

  /**
   * <p>Parses the <code>inputSpec</code> as a KeyValueStore xml configuration file.
   * Adds the name-to-KeyValueStore bindings defined there to this job.
   * See the user guide on "Specifying KeyValueStore bindings from the Command Line"
   * for xml format for this config file.</p>
   *
   * <p>The caller of this method is responsible for closing the InputStream
   * argument.</p>
   *
   * @param inputSpec the InputStream that provides the XML file specifying the input stores.
   * @return This builder instance so you may chain configuration method calls.
   * @throws IOException if there is an error reading or parsing the input spec.
   */
  @SuppressWarnings("unchecked")
  public final T withStoreBindings(InputStream inputSpec) throws IOException {
    XmlKeyValueStoreParser parser = XmlKeyValueStoreParser.get(getConf());
    Map<String, KeyValueStore<?, ?>> newStores = parser.loadStoresFromXml(inputSpec);
    mergeStores(mBoundStores, newStores);
    return (T) this;
  }

  /**
   * Sets the base Hadoop configuration used to build the MapReduce job.
   *
   * <p>Classes that override this method should call super.withConf or override
   * {@link MapReduceJobBuilder#getConf() getConf}.</p>
   *
   * @param conf Base Hadoop configuration used to build the MapReduce job.
   * @return this.
   */
  @SuppressWarnings("unchecked")
  public T withConf(Configuration conf) {
    mConf = Preconditions.checkNotNull(conf);
    return (T) this;
  }

  /**
   * Gets the base Hadoop configuration used to build the MapReduce job.
   *
   * <p>Classes should override this method if they also override
   * {@link MapReduceJobBuilder#withConf(Configuration) withConf}.</p>
   *
   * @return the base Hadoop configuration used to build the MapReduce job.
   */
  public final Configuration getConf() {
    return mConf;
  }

  /**
   * Configures a Hadoop MR job.
   *
   * @param job The Hadoop MR job.
   * @throws IOException If there is an error.
   */
  protected void configureJob(Job job) throws IOException {
    configureInput(job);
    configureOutput(job);
    configureMapper(job);
    configureCombiner(job);
    configureReducer(job);
    configureStores(job);
    configureJars(job);

    // Obtain delegation tokens for the job
    TableMapReduceUtil.initCredentials(job);
  }

  /**
   * Configures the input for the job.
   *
   * @param job The job to configure.
   * @throws IOException If there is an error.
   */
  protected void configureInput(Job job) throws IOException {
    getJobInput().configure(job);
  }

  /**
   * Configures the MapReduce mapper for the job.
   *
   * @param job The Hadoop MR job.
   * @throws IOException If there is an error.
   */
  protected void configureMapper(Job job) throws IOException {
    // Set the map class in the job configuration.
    final KijiMapper<?, ?, ?, ?> mapper = getMapper();
    if (null == mapper) {
      throw new JobConfigurationException("Must specify a mapper");
    }
    if (mapper instanceof Configurable) {
      ((Configurable) mapper).setConf(job.getConfiguration());
    }
    job.setMapperClass(((Mapper<?, ?, ?, ?>) mapper).getClass());

    // Set the map output key and map output value types in the job configuration.
    job.setMapOutputKeyClass(mapper.getOutputKeyClass());
    job.setMapOutputValueClass(mapper.getOutputValueClass());

    configureAvro(job, mapper);
    configureHTableInput(job, mapper);
  }

  /**
   * Configures the job with any Avro reader or writer schemas specified by the mapper class.
   *
   * <p>If the job's mapper class uses AvroKey as the job's input key class, it should
   * have implemented the AvroKeyReader interface to specify the reader schema for the
   * input key.  Likewise, if it uses AvroValue as the job's input value class, it should
   * have implemented the AvroValueReader interface.</p>
   *
   * <p>If the job's mapper class uses AvroKey as the output key class, it should
   * have implemented the AvroKeyWriter interface to specify the writer schema for the
   * output key.  Likewise, if it uses AvroValue as the output value class, it should have
   * implemented the AvroValueWriter interface.</p>
   *
   * <p>This method makes sure those interfaces were implemented correctly, uses them to
   * fetch the reader/writer schemas as necessary, and sets them in the Job configuration
   * so the Avro input format and serialization framework can access them.</p>
   *
   * @param job The job to configure.
   * @param mapper The Kiji mapper the job is configured to run.
   * @throws IOException If the Avro schemas cannot be configured.
   */
  protected void configureAvro(Job job, KijiMapper<?, ?, ?, ?> mapper) throws IOException {
    // If the user has specified particular reader schemas for the records of the input,
    // put it in the job configuration.
    Schema inputKeyReaderSchema = AvroMapReduce.getAvroKeyReaderSchema(mapper);
    if (null != inputKeyReaderSchema) {
      LOG.info("Setting reader schema for the map input key to: " + inputKeyReaderSchema);
      AvroJob.setInputKeySchema(job, inputKeyReaderSchema);
    }
    Schema inputValueReaderSchema = AvroMapReduce.getAvroValueReaderSchema(mapper);
    if (null != inputValueReaderSchema) {
      LOG.info("Setting reader schema for the map input value to: " + inputValueReaderSchema);
      AvroJob.setInputValueSchema(job, inputValueReaderSchema);
    }

    // Set the output writer schemas in the job configuration (if specified).
    Schema outputKeyWriterSchema = AvroMapReduce.getAvroKeyWriterSchema(mapper);
    if (null != outputKeyWriterSchema) {
      if (!AvroKey.class.isAssignableFrom(job.getMapOutputKeyClass())) {
        throw new JobConfigurationException(mapper.getClass().getName()
            + ".getAvroKeyWriterSchema() returned a non-null Schema"
            + " but the output key class was not AvroKey.");
      }
      LOG.info("Setting avro serialization for map output key schema: " + outputKeyWriterSchema);
      AvroJob.setMapOutputKeySchema(job, outputKeyWriterSchema);
    }
    Schema outputValueWriterSchema = AvroMapReduce.getAvroValueWriterSchema(mapper);
    if (null != outputValueWriterSchema) {
      if (!AvroValue.class.isAssignableFrom(job.getMapOutputValueClass())) {
        throw new JobConfigurationException(mapper.getClass().getName()
            + ".getAvroValueWriterSchema() returned a non-null Schema"
            + " but the output value class was not AvroValue.");
      }
      LOG.info("Setting avro serialization for map output value schema: "
          + outputValueWriterSchema);
      AvroJob.setMapOutputValueSchema(job, outputValueWriterSchema);
    }
  }

  /**
   * Configures HTable input for the job if the mapper is an HTableReader.
   *
   * <p>If the mapper the job is configured to run is reading from an HBase table
   * (HTable), this method will make sure the mapper implements the HTableReader interface
   * and use its methods to configure the table scan specification required for the
   * HTableInputFormat.</p>
   *
   * <p>A mapper that reads its input from an HTable needs to specify the Scan descriptor
   * that describes what subset of rows and column cells should be processed.  The mapper
   * communicates this by implementing the methods of the HTableReader interface.  This
   * method calls the methods of that interface on the job's mapper and sets Scan
   * descriptor into the job configuration so the HTableInputFormat can read it.</p>
   *
   * @param job The job to configure.
   * @param mapper The Kiji mapper the job is configured to run.
   * @throws IOException If the HTable input cannot be configured.
   */
  protected void configureHTableInput(Job job, KijiMapper<?, ?, ?, ?> mapper) throws IOException {
    if (mapper instanceof HTableReader) {
      HTableReader htableReader = (HTableReader) mapper;
      Scan hbaseInputTableScan = htableReader.getInputHTableScan(job.getConfiguration());
      if (null == hbaseInputTableScan) {
        return;
      }
      LOG.debug("Configuring HTable scan: " + hbaseInputTableScan.toString());
      GenericTableMapReduceUtil.initTableScan(hbaseInputTableScan, job);
    }
  }

  /**
   * Configures the MapReduce combiner for the job.
   *
   * @param job The Hadoop MR job.
   */
  protected void configureCombiner(Job job) {
    final KijiReducer<?, ?, ?, ?> combiner = getCombiner();
    if (null == combiner) {
      LOG.debug("No combiner provided.");
      return;
    }
    if (combiner instanceof Configurable) {
      ((Configurable) combiner).setConf(job.getConfiguration());
    }
    job.setCombinerClass(((Reducer<?, ?, ?, ?>) combiner).getClass());
  }

  /**
   * Configures the MapReduce reducer for the job.
   *
   * @param job The Hadoop MR job.
   * @throws IOException If there is an error.
   */
  protected void configureReducer(Job job) throws IOException {
    final KijiReducer<?, ?, ?, ?> reducer = getReducer();
    if (null == reducer) {
      LOG.info("No reducer provided. This will be a map-only job");
      job.setNumReduceTasks(0);

      // Set the job output key/value classes based on what the map output key/value classes were
      // since this a map-only job.
      job.setOutputKeyClass(job.getMapOutputKeyClass());
      Schema mapOutputKeySchema = AvroJob.getMapOutputKeySchema(job.getConfiguration());
      if (null != mapOutputKeySchema) {
        AvroJob.setOutputKeySchema(job, mapOutputKeySchema);
      }
      job.setOutputValueClass(job.getMapOutputValueClass());
      Schema mapOutputValueSchema = AvroJob.getMapOutputValueSchema(job.getConfiguration());
      if (null != mapOutputValueSchema) {
        AvroJob.setOutputValueSchema(job, mapOutputValueSchema);
      }
      return;
    }
    if (reducer instanceof Configurable) {
      ((Configurable) reducer).setConf(job.getConfiguration());
    }
    job.setReducerClass(reducer.getClass());

    // Set output key class.
    Class<?> outputKeyClass = reducer.getOutputKeyClass();
    job.setOutputKeyClass(outputKeyClass);
    Schema outputKeyWriterSchema = AvroMapReduce.getAvroKeyWriterSchema(reducer);
    if (AvroKey.class.isAssignableFrom(outputKeyClass)) {
      if (null == outputKeyWriterSchema) {
        throw new JobConfigurationException(
            "Using AvroKey output, but a writer schema was not provided. "
            + "Did you forget to implement AvroKeyWriter in your KijiReducer?");
      }
      AvroJob.setOutputKeySchema(job, outputKeyWriterSchema);
    } else if (null != outputKeyWriterSchema) {
      throw new JobConfigurationException(reducer.getClass().getName()
          + ".getAvroKeyWriterSchema() returned a non-null Schema"
          + " but the output key class was not AvroKey.");
    }

    // Set output value class.
    Class<?> outputValueClass = reducer.getOutputValueClass();
    job.setOutputValueClass(outputValueClass);
    Schema outputValueWriterSchema = AvroMapReduce.getAvroValueWriterSchema(reducer);
    if (AvroValue.class.isAssignableFrom(outputValueClass)) {
      if (null == outputValueWriterSchema) {
        throw new JobConfigurationException(
            "Using AvroValue output, but a writer schema was not provided. "
            + "Did you forget to implement AvroValueWriter in your KijiReducer?");
      }
      AvroJob.setOutputValueSchema(job, outputValueWriterSchema);
    } else if (null != outputValueWriterSchema) {
      throw new JobConfigurationException(reducer.getClass().getName()
          + ".getAvroValueWriterSchema() returned a non-null Schema"
          + " but the output value class was not AvroValue.");
    }
  }

  /**
   * Configures the output for the job.
   *
   * @param job The job to configure.
   * @throws IOException If there is an error.
   */
  protected void configureOutput(Job job) throws IOException {
    if (null == mJobOutput) {
      throw new JobConfigurationException("Must specify job output.");
    }
    mJobOutput.configure(job);
  }

  /**
   * Configures the key-value stores to be attached to the job.
   *
   * <p>This adds the key-value stores defined by the user and the
   * job components (producer, gatherer, etc) to the job configuration.
   * </p>
   *
   * @param job The job to configure.
   * @throws IOException if there is an error configuring the stores. This
   *     may include, e.g., the case where a store is required but not defined.
   */
  protected final void configureStores(Job job) throws IOException {
    KeyValueStoreConfigValidator.get().bindAndValidateRequiredStores(
        getRequiredStores(), mBoundStores);
    KeyValueStoreConfigSerializer.get().addStoreMapToConfiguration(
        mBoundStores, job.getConfiguration());
  }

  /**
   * Adds all KeyValueStore mappings from src to the 'dest' map.
   * Warns the user if the same store name is defined multiple times with a different
   * store implementation.
   *
   * @param dest the map to modify; will contain the union of its current data and
   *     all mappings in src.
   * @param src an additional set of KeyValueStore mappings to apply.
   */
  protected final void mergeStores(Map<String, KeyValueStore<?, ?>> dest,
      Map<String, KeyValueStore<?, ?>> src) {
    for (Map.Entry<String, KeyValueStore<?, ?>> entry : src.entrySet()) {
      String name = entry.getKey();
      KeyValueStore<?, ?> store = entry.getValue();

      if (dest.containsKey(name)) {
        KeyValueStore<?, ?> existing = dest.get(name);
        if (null != existing && !existing.equals(store)) {
          LOG.warn("KeyValueStore '" + name + "' is bound to [" + existing
              + "]; overwriting with a different default [" + store + "]");
        }
      }

      dest.put(name, store);
    }
  }

  /**
   * Method for job components to declare required KeyValueStore entries (and their default
   * implementations).
   *
   * <p>Classes inheriting from MapReduceJobBuilder should override this method.</p>
   *
   * @return a map of required names to default KeyValueStore implementations to add. These will
   *     be used to augment the existing map if any names are not defined by withStore().
   * @throws IOException if there is an error configuring stores.
   */
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return Collections.<String, KeyValueStore<?, ?>>emptyMap();
  }

  /**
   * Configures the jars to be added to the distributed cache during the job.
   *
   * <p>This adds the jar class containing the mapper to the job via
   * <code>setJarByClass()</code> and adds the jars kiji depends on from the kiji release
   * lib directory, which is assumed to be the same directory as the kiji jar.</p>
   *
   * @param job The job to configure.
   * @throws IOException If there is an error reading the jars from the file system.
   */
  protected void configureJars(Job job) throws IOException {
    job.setJarByClass(getJarClass());

    final String jobLibProperty = System.getProperty(JOB_LIB_PROPERTY, null);
    if (jobLibProperty != null) {
      LOG.debug("Adding lib/*.jar directory from system property {}: '{}'.",
          JOB_LIB_PROPERTY, jobLibProperty);
      addJarDirectory(jobLibProperty);
    }

    // Add the jars that the user has added via addJarDirectory() method calls.
    // We are doing this BEFORE we add the Kiji's own dependency jars,
    // so that users can "override" dependencies of Kiji, such as Avro.
    for (Path jarDirectory : mJarDirectories) {
      LOG.debug("Adding directory of user jars to the distributed cache: " + jarDirectory);
      DistributedCacheJars.addJarsToDistributedCache(job, jarDirectory);
    }

    final boolean addClasspathToDCache =
        Boolean.parseBoolean(System.getProperty(ADD_CLASSPATH_TO_JOB_DCACHE_PROPERTY, "false"));
    if (addClasspathToDCache) {
      final List<Path> jarFiles = Lists.newArrayList();
      for (String cpEntry : System.getProperty("java.class.path").split(":")) {
        if (cpEntry.endsWith(".jar")) {
          jarFiles.add(new Path("file:" + cpEntry));  // local files
        }
      }
      DistributedCacheJars.addJarsToDistributedCache(job, jarFiles);
    }

    // We release kiji-schema to a directory called KIJI_HOME, and the jars for kiji and all its
    // dependencies live in KIJI_HOME/lib.  Add everything in this lib directory to the
    // distributed cache.
    // And the same for kiji-mapreduce.
    try {
      final File kijiSchemaJarPath = new File(Jars.getJarPathForClass(Kiji.class));
      final File kijiSchemaDependencyLibDir = kijiSchemaJarPath.getParentFile();

      LOG.debug("Adding kiji-schema dependency jars to the distributed cache of the job: "
          + kijiSchemaDependencyLibDir);
      DistributedCacheJars.addJarsToDistributedCache(
          job, new Path("file:" + kijiSchemaDependencyLibDir));

    } catch (ClassNotFoundException e) {
      LOG.warn("The kiji-schema jar could not be found, so no kiji dependency jars will be "
          + "loaded onto the distributed cache.");
    }

    try {
      final File kijiMRJarPath = new File(Jars.getJarPathForClass(MapReduceJobBuilder.class));
      final File kijiMRDependencyLibDir = kijiMRJarPath.getParentFile();
      LOG.debug("Adding kiji-mapreduce dependency jars to the distributed cache of the job: "
          + kijiMRDependencyLibDir);
      DistributedCacheJars.addJarsToDistributedCache(
          job, new Path("file:" + kijiMRDependencyLibDir));

    } catch (ClassNotFoundException e) {
      LOG.warn("The kiji-mapreduce jar could not be found, so no kiji dependency jars will be "
          + "loaded onto the distributed cache.");
    }

    // Finally, make sure HBase's own dependencies are bundled too.
    // This uses the "Generic" TableMapReduceUtil that is packaged in KijiSchema,
    // and extends HBase's own TableMapReduceUtil (in the same package) to work
    // around a few bugs therein.
    GenericTableMapReduceUtil.addAllDependencyJars(job);

    // Ensure jars we place on the dcache take precedence over Hadoop + HBase lib jars.
    KijiMRPlatformBridge.get().setUserClassesTakesPrecedence(job, true);
  }

  /**
   * Wraps a Hadoop MR job in a <code>MapReduceJob</code>.
   *
   * @param job The Hadoop MR job.
   * @return The built <code>MapReduceJob</code>.
   */
  protected abstract KijiMapReduceJob build(Job job);

  /**
   * Gets the input to configure the job with.
   *
   * @return The job input.
   */
  protected abstract MapReduceJobInput getJobInput();

  /**
   * Gets an instance of the MapReduce mapper to be used for this job.
   *
   * @return An instance of the mapper.
   */
  protected abstract KijiMapper<?, ?, ?, ?> getMapper();

  /**
   * Gets an instance of the MapReduce combiner to be used for this job.
   *
   * @return An instance of the combiner, or null if this job should not use a combiner.
   */
  protected abstract KijiReducer<?, ?, ?, ?> getCombiner();

  /**
   * Gets an instance of the MapReduce reducer to be used for this job.
   *
   * @return An instance of the reducer, or null if this job should be map-only.
   */
  protected abstract KijiReducer<?, ?, ?, ?> getReducer();

  /**
   * Returns a class that should be used to determine which Java jar archive will be
   * automatically included on the classpath of the MR tasks.
   *
   * @return A class contained in the jar to be included on the MR classpath.
   */
  protected abstract Class<?> getJarClass();
}
