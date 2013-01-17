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

package org.kiji.mapreduce.kvstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;

/**
 * Abstract base class that provides common functionality to file-backed
 * KeyValueStore implementations, such as the ability to use directory
 * globbing, the DistributedCache, etc.
 *
 * <p>When specifying a file-backed KeyValueStore in a kvstores XML file,
 * the following properties may be specified:</p>
 * <ul>
 *   <li><tt>paths</tt> - A comma-delimited list of HDFS paths to files that
 *       should be included in the KeyValueStore.</li>
 *   <li><tt>dcache</tt> - A boolean value ("<tt>true</tt>" or "<tt>false</tt>")
 *       specifying whether the input files should be sent to tasks via the
 *       DistributedCache or not. (The default is <tt>true</tt>.)</li>
 * </ul>
 *
 * @param <K> the key type expected to be implemented by the keys to this store.
 * @param <V> the value type expected to be accessed by keys to this store.
 */
@ApiAudience.Public
public abstract class FileKeyValueStore<K, V> extends KeyValueStore<K, V>
    implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(FileKeyValueStore.class);

  /**
   * By default, it is assumed that the user wants to load this KeyValueStore
   * through the DistributedCache.
   */
  public static final boolean USE_DCACHE_DEFAULT = true;

  private FileKeyValueArrayStore<K, V> mStore;

  /**
   * Input configuration options that configure a file-backed KeyValueStore.
   *
   * @param <T> the type of the options class.
   */
  public static class Options<T extends Options<?>> {
    private Configuration mConf;
    private List<Path> mInputPaths;
    private boolean mUseDCache;

    /** Default constructor. */
    public Options() {
      mInputPaths = new ArrayList<Path>();
      mUseDCache = USE_DCACHE_DEFAULT;
      mConf = new Configuration();
    }

    /**
     * Sets the Hadoop configuration instance to use.
     *
     * @param conf The configuration.
     * @return This options instance.
     */
    @SuppressWarnings("unchecked")
    public T withConfiguration(Configuration conf) {
      mConf = conf;
      return (T) this;
    }

    /**
     * Adds a path to the list of files to load.
     *
     * @param path The input file/directory path.
     * @return This options instance.
     */
    @SuppressWarnings("unchecked")
    public T withInputPath(Path path) {
      mInputPaths.add(path);
      return (T) this;
    }

    /**
     * Replaces the current list of files to load with the set of files
     * specified as an argument.
     *
     * @param paths The input file/directory paths.
     * @return This options instance.
     */
    @SuppressWarnings("unchecked")
    public T withInputPaths(List<Path> paths) {
      mInputPaths.clear();
      mInputPaths.addAll(paths);
      return (T) this;
    }

    /**
     * Sets a flag indicating the use of the DistributedCache to distribute
     * input files.
     *
     * @param enabled true if the DistributedCache should be used, false otherwise.
     * @return This options instance.
     */
    @SuppressWarnings("unchecked")
    public T withDistributedCache(boolean enabled) {
      mUseDCache = enabled;
      return (T) this;
    }

    /**
     * Gets the configuration instance.
     *
     * @return The configuration.
     */
    public Configuration getConfiguration() {
      return mConf;
    }

    /**
     * Gets the list of paths to the files/directories to load.
     *
     * @return The input file path.
     */
    public List<Path> getInputPaths() {
      return mInputPaths;
    }

    /**
     * Gets the flag that determines whether the DistributedCache should be used.
     *
     * @return true if the DistributedCache should be used for the input files.
     */
    public boolean getUseDistributedCache() {
      return mUseDCache;
    }
  }

  /**
   * Constructor for the FileKeyValueStore.
   *
   * @param store the FileKeyValueArrayStore to delegate calls to.
   */
  public FileKeyValueStore(FileKeyValueArrayStore<K, V> store) {
    mStore = store;
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mStore.setConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mStore.getConf();
  }

  /**
   * Gets the array store used to delegate calls to.
   *
   * @return the array store used to delegate calls to.
   */
  protected FileKeyValueArrayStore<K, V> getArrayStore() {
    return mStore;
  }

  /**
   * Sets the array store used to delegate calls to.
   *
   * @param store the array store used to delegate calls to.
   */
  protected void setArrayStore(FileKeyValueArrayStore<K, V> store) {
    mStore = store;
  }

  /**
   * Adds an input path to the list of files which should be loaded into this
   * KeyValueStore.
   *
   * @param path the path to the file or directory to load.
   */
  public void addInputPath(Path path) {
    mStore.addInputPath(path);
  }

  /**
   * Sets the input path which specifies the file or directory that should be
   * loaded as the input for this KeyValueStore. This method overwrites the
   * values set by any prior calls to setInputPath() or addInputPath().
   *
   * @param path the path to the file or directory to load.
   */
  public void setInputPath(Path path) {
    mStore.setInputPath(path);
  }

  /**
   * Sets the input paths which specify the files or directories that should
   * be loaded as the input for this KeyValueStore. This method overwrites
   * any previous calls to setInputPath() or addInputPath().
   *
   * @param paths the list of paths to files or directories to load.
   */
  public void setInputPaths(List<Path> paths) {
    mStore.setInputPaths(paths);
  }

  /**
   * Returns the set of input path(s) to be loaded by this KeyValueStore.
   * If this is called within a MapReduce task, these may refer to local paths
   * on disk. This may include directories and wildcards and other user-entered data.
   *
   * @return an unmodifiable list of input paths, backed by the underlying collection
   *     within this KeyValueStore.
   */
  public List<Path> getInputPaths() {
    return mStore.getInputPaths();
  }

  /**
   * Returns the set of input path(s) that should be actually opened for read.
   * This set of paths may be on local disk (e.g., if the DistributedCache was used
   * to transmit the files), or in HDFS. This will not contain directory names nor
   * globs; it is expanded to the literal set of files to open.
   *
   * @return an unmodifiable list of input paths, backed by the underlying collection
   *     within this KeyValueStore.
   * @throws IOException if there is an error communicating with the underlying
   *     FileSystem while expanding paths and globs.
   */
  public List<Path> getExpandedInputPaths() throws IOException {
    return mStore.getExpandedInputPaths();
  }

  /**
   * Sets a flag that enables or disables the use of the DistributedCache to manage
   * the distribution of the input files to the tasks that back this KeyValueStore.
   * This has no effect for KeyValueStores that are not used in MapReduce jobs.
   * <b>The DistributedCache is enabled by default.</b> This should be disabled if your files
   * are particularly large (the DCache limit is 10 GB per job, by default), but take
   * care that too many mappers do not overwhelm the same HDFS nodes.
   *
   * <p>(Note that most file-backed KeyValueStore implementations read the entire
   * set of input files into memory; 10 GB or more is unlikely to fit in the heap.)</p>
   *
   * @param enable a boolean that indicates whether the use of the distributed cache
   *     should be enabled.
   */
  public void setEnableDistributedCache(boolean enable) {
    mStore.setEnableDistributedCache(enable);
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.storeToConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.initFromConf(conf);
  }
}

