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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStore;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.util.Lists;

/**
 * Abstract base class that provides common functionality to file-backed
 * KeyValueStore implementations, such as the ability to use directory
 * globbing, the DistributedCache, etc.
 *
 * @param <K> the key type expected to be implemented by the keys to this store.
 * @param <V> the value type expected to be accessed by keys to this store.
 */
@ApiAudience.Public
public abstract class FileKeyValueArrayStore<K, V> extends KeyValueStore<K, List<V>>
    implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(
      FileKeyValueStore.class.getName());

  /**
   * KeyValueStoreConfiguration variable specifying the prefix for DCache
   * target file names.  If empty, then the DCache is disabled.
   */
  public static final String CONF_DCACHE_PREFIX = "dcache.suffix";

  /**
   * KeyValueStoreConfiguration variable specifying the set of input paths,
   * as a comma-delimited list.
   */
  public static final String CONF_PATHS = "paths";

  /**
   * KeyValueStoreConfiguration variable specifying the
   * maximum number of values to associate with each key.
   */
  public static final String CONF_MAX_VALUES = "max.values";

  /** The Hadoop configuration. */
  private Configuration mConf;

  /** True if we should distribute the input files via the DistributedCache. */
  private boolean mUseDCache;

  /** Max number of values to read. */
  private long mMaxValues;

  /** Files stored in the distributed cache have this as their prefix. */
  private String mDCachePrefix;

  /** List of input paths to files to include in the store. */
  private List<Path> mInputPaths;

  /**
   * Constructor that accepts an Options argument to configure it.
   *
   * @param options the specifications for this FileKeyValueArrayStore.
   */
  public FileKeyValueArrayStore(
      FileKeyValueStore.Options<? extends FileKeyValueStore.Options<?>> options) {
    this(options, Long.MAX_VALUE);
  }

  /**
   * Constructor that accepts an Options argument to configure it.
   *
   * @param options the specifications for this FileKeyValueStore.
   * @param maxValues the maximum number of values to associate with each key.
   */
  public FileKeyValueArrayStore(
      FileKeyValueStore.Options<? extends FileKeyValueStore.Options<?>> options,
      Long maxValues) {
    setConf(options.getConfiguration());
    setEnableDistributedCache(options.getUseDistributedCache());
    setInputPaths(options.getInputPaths());
    setMaxValues(maxValues);
    mDCachePrefix = "";
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /**
   * Gets the maximum number of values to associate with each key.
   *
   * @return The maximum number of values to associate with each key.
   */
  public long getMaxValues() {
    return mMaxValues;
  }

  /**
   * Sets the maximum number of values to associate with each key.
   *
   * @param maxValues The maximum number of values to associate with each key.
   */
  public void setMaxValues(long maxValues) {
    mMaxValues = maxValues;
  }
  /**
   * Adds an input path to the list of files which should be loaded into this
   * KeyValueStore.
   *
   * @param path the path to the file or directory to load.
   */
  public void addInputPath(Path path) {
    mInputPaths.add(path);
  }

  /**
   * Sets the input path which specifies the file or directory that should be
   * loaded as the input for this KeyValueStore. This method overwrites the
   * values set by any prior calls to setInputPath() or addInputPath().
   *
   * @param path the path to the file or directory to load.
   */
  public void setInputPath(Path path) {
    mInputPaths.clear();
    addInputPath(path);
  }

  /**
   * Sets the input paths which specify the files or directories that should
   * be loaded as the input for this KeyValueStore. This method overwrites
   * any previous calls to setInputPath() or addInputPath().
   *
   * @param paths the list of paths to files or directories to load.
   */
  public void setInputPaths(List<Path> paths) {
    mInputPaths = new ArrayList<Path>(paths);
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
    return Collections.unmodifiableList(mInputPaths);
  }

  /**
   * An aggregator for use with Lists.foldLeft() that expands a list of paths that
   * may include directories to include only files; directories are expanded to multiple
   * file entries that are the files in this directory.
   */
  private class DirExpandAggregator extends Lists.Aggregator<Path, List<Path>> {
    // NOTE: This class stands ready to be factored out to assist other places
    // in Kiji where we need to expand a list of files, dirs and globs into just a list
    // of files.

    /** Last exception encountered during file stat lookups for the input paths. */
    private IOException mLastExn;

    /**
     * For each input path, modify the 'outputs' list to include the path
     * itself (if it is a file), or all the files in the directory (if it
     * is a directory). Also expands globs with FileSystem.globStatus().
     *
     * @param inputPath the input path to expand.
     * @param outputs list of output paths being accumulated.
     * @return the 'outputs' list.
     */
    @Override
    public List<Path> eval(Path inputPath, List<Path> outputs) {
      try {
        FileSystem fs = inputPath.getFileSystem(getConf());
        FileStatus[] matches = fs.globStatus(inputPath);
        if (null == matches) {
          mLastExn = new IOException("No such input path: " + inputPath);
        } else if (matches.length == 0) {
          mLastExn = new IOException("Input pattern \"" + inputPath + "\" matches 0 files.");
        } else {
          for (FileStatus match : matches) {
            if (match.isDir()) {
              // Match all the files in this dir, except the "bonus" files generated by
              // MapReduce.
              for (FileStatus subFile : fs.listStatus(match.getPath(),
                  new org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter())) {
                outputs.add(subFile.getPath());
                LOG.debug("Added file: " + subFile.getPath());
              }
            } else {
              // Just a file; add directly.
              outputs.add(match.getPath());
              LOG.debug("Added file: " + match.getPath());
            }
          }
        }
      } catch (IOException ioe) {
        mLastExn = ioe;
      }
      return outputs;
    }

    /**
     * Returns the last exception encountered during FS operation while expanding
     * directories.
     *
     * @return the last exception encountered, or null if none was encountered.
     */
    private IOException getLastException() {
      return mLastExn;
    }
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
    // If we've read a bunch of files from the DistributedCache's local dir,
    // no further unglobbing is necessary. Just return the values.
    if (!mDCachePrefix.isEmpty()) {
      return Collections.unmodifiableList(mInputPaths);
    }

    // Otherwise, these are "raw" user inputs. Unglob and expand them.
    DirExpandAggregator expander = new DirExpandAggregator();

    List<Path> actualInputPaths = Lists.distinct(Lists.foldLeft(
        new ArrayList<Path>(), mInputPaths, expander));

    IOException savedException = expander.getLastException();
    if (null != savedException) {
      // Rethrow the saved exception from this context.
      throw savedException;
    }

    return Collections.unmodifiableList(actualInputPaths);
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
    mUseDCache = enable;
  }

  /**
   * If the cache URI prefix is already set, return this value. Otherwise create
   * a new unique cache URI prefix.
   *
   * @return the DistributedCache URI prefix for files used by this store.
   */
  private String getCachePrefix() {
    if (mDCachePrefix.isEmpty()) {
      // We need to put the files for this KVStore into the distributed cache. They
      // should be given symlink names that do not conflict with the names associated
      // with other KeyValueStores. Pick a symlink prefix that is unique to this store.
      long prefixId = System.currentTimeMillis() ^ (((long) this.hashCode()) << 8);
      StringBuilder sb = new StringBuilder();
      Formatter formatter = new Formatter(sb, Locale.US);
      formatter.format("%08x", prefixId);
      String newPrefix = sb.toString();
      LOG.debug("This KeyValueStore uses Distributed cache files in namespace: " + newPrefix);
      return newPrefix;
    } else {
      return mDCachePrefix; // Prefix is already set.
    }
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mInputPaths.isEmpty()) {
      throw new IOException("Required attribute not set: input path");
    }

    conf.setLong(CONF_MAX_VALUES, mMaxValues);

    if (mUseDCache && !"local".equals(conf.getDelegate().get("mapred.job.tracker", ""))) {
      // If we're scheduled to use the distributed cache, and we're not in the LocalJobRunner,
      // add these files to the DistributedCache.

      // TODO(WIBI-1653): This does not handle any sort of MapperTester, etc.
      // We need a separate flag that tells this to ignore mUseDCache if we're in a test
      // environment, and just use the original input file specs.
      final String dCachePrefix = getCachePrefix();

      // Associate this randomly chosen prefix id with this KVStore implementation.
      conf.set(CONF_DCACHE_PREFIX, dCachePrefix);

      // Add the input paths to the DistributedCache and translate path names.
      int uniqueId = 0;
      for (Path inputPath : getExpandedInputPaths()) {
        FileSystem fs = inputPath.getFileSystem(conf.getDelegate());
        Path absolutePath = inputPath.makeQualified(fs);

        String uriStr = absolutePath.toString() + "#" + dCachePrefix + "-" + uniqueId;
        LOG.debug("Adding to DistributedCache: " + uriStr);
        uniqueId++;
        try {
          DistributedCache.addCacheFile(new URI(uriStr), conf.getDelegate());
        } catch (URISyntaxException use) {
          throw new IOException("Could not construct URI for file: " + uriStr, use);
        }
      }

      // Ensure that symlinks are created for cached files.
      DistributedCache.createSymlink(conf.getDelegate());

      // Now save the cache prefix into the local state.  We couldn't set this earlier,
      // because we wanted getExpandedInputPaths() to actually unglob things. That
      // function will behave differently if mDCachePrefix is already initialized.
      mDCachePrefix = dCachePrefix;
    } else {
      // Just put the regular HDFS paths in the Configuration.
      conf.setStrings(CONF_PATHS,
          Lists.toArray(Lists.map(mInputPaths, new Lists.ToStringFn<Path>()), String.class));
    }
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    setConf(conf.getDelegate());
    mDCachePrefix = conf.get(CONF_DCACHE_PREFIX, "");
    LOG.debug("Input dCachePrefix: " + mDCachePrefix);
    mMaxValues = conf.getLong(CONF_MAX_VALUES, Long.MAX_VALUE);

    if (mDCachePrefix.isEmpty()) {
      // Read an ordinary list of files from the Configuration.
      // These may include directories and globs to expand.
      mInputPaths = Lists.map(Arrays.asList(conf.getStrings(CONF_PATHS,
          new String[0])), new Lists.Func<String, Path>() {
            @Override
            public Path eval(String in) {
              LOG.debug("File input: " + in);
              return new Path(in);
            }
          });
    } else {
      // Use the dcache prefix to get the names of the files for this store.
      // The symlinks are already present in the working dir of the task.
      final FileSystem localFs = FileSystem.getLocal(conf.getDelegate());
      FileStatus[] statuses = localFs.globStatus(new Path(mDCachePrefix + "-*"));
      if (null == statuses || statuses.length == 0) {
        throw new IOException("No files associated with the job in the DistributedCache");
      }

      // Get the (absolute) input file paths to use.
      mInputPaths = Lists.map(Arrays.asList(statuses), new Lists.Func<FileStatus, Path>() {
        @Override
        public Path eval(FileStatus status) {
          Path out = status.getPath().makeQualified(localFs);
          LOG.debug("Loaded from DistributedCache: " + out);
          return out;
        }
      });
    }
  }
}

