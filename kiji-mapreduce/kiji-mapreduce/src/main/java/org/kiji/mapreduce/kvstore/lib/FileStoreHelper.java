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

package org.kiji.mapreduce.kvstore.lib;

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
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;
import org.kiji.mapreduce.util.Lists;

/**
 * Helper class that manages filenames and distributed cache functionality
 * for KeyValueStore implementations that work with files or collections
 * of files.
 *
 * <p>Your KeyValueStore can use a FileStoreHelper to manage all aspects of
 * configuration of and deserialization regarding file names in a MapReduce
 * job.</p>
 *
 * <p>Create a FileStoreHelper.Builder object using the builder() method;
 * your own KeyValueStore's Builder should use composition to delegate
 * responsibility for Configuration, file, and distributed cache control
 * to this one. You should use this object's storeToConf() and initFromConf()
 * methods within your own KeyValueStore's storeToConf() and initFromConf()
 * methods.</p>
 *
 * <p>When reading files, use getExpandedInputPaths() to get a complete list
 * of files to read. If the user has enabled the distributed cache, you will
 * receive a set of local files to read. Otherwise, the initial HDFS paths
 * will be used.</p>
 */
@ApiAudience.Public
public final class FileStoreHelper implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(
      FileStoreHelper.class.getName());

  /**
   * Configuration key for the KeyValueStore definition that sets whether input files are
   * stored in the DistributedCache. If empty, then DCache is disabled. If non-empty,
   * then DCache target file names are expected to be prefixed by the string in this
   * configuration key.
   */
  public static final String CONF_DCACHE_PREFIX_KEY = "dcache.prefix";

  /**
   * Boolean flag used in XML Configuration files only to state that the files
   * specified are
   * HDFS files, but should be loaded into the DistributedCache as part
   * of the job. This flag is not recorded as part of addToConfiguration().
   */
  public static final String CONF_USE_DCACHE_KEY = "dcache";
  // This flag sets mUseDCache.

  /**
   * By default, it is assumed that the user wants to load this KeyValueStore
   * through the DistributedCache.
   */
  public static final boolean USE_DCACHE_DEFAULT = true;

  /**
   * Suffix for the KeyValueStore definition that is set to the list of
   * input paths. This may be multiple comma-delimited paths.
   */
  public static final String CONF_PATHS_KEY = "paths";

  /** The Hadoop configuration. */
  private Configuration mConf;

  /** True if we should distribute the input files via the DistributedCache. */
  private boolean mUseDCache;

  /** Files stored in the distributed cache have this as their prefix. */
  private String mDCachePrefix;

  /** List of input paths to files to include in the store. */
  private List<Path> mInputPaths;

  /**
   * A class that builds configured FileStoreHelper instances.
   *
   * <p>This object is not exposed to users directly. It is used via composition
   * in other (FooFile)KeyValueStore.Builder instances. If you add a method here,
   * you should reflect this via composition in the other file-backed store builder
   * APIs.</p>
   */
  @ApiAudience.Public
  public static final class Builder {
    private Configuration mConf;
    private List<Path> mInputPaths;
    private boolean mUseDCache;

    /** private constructor. */
    private Builder() {
      mInputPaths = new ArrayList<Path>();
      mUseDCache = USE_DCACHE_DEFAULT;
      mConf = new Configuration();
    }

    /**
     * Sets the Hadoop configuration instance to use.
     *
     * @param conf The configuration.
     * @return This builder instance.
     */
    public Builder withConfiguration(Configuration conf) {
      mConf = conf;
      return this;
    }

    /**
     * Adds a path to the list of files to load.
     *
     * @param path The input file/directory path.
     * @return This builder instance.
     */
    public Builder withInputPath(Path path) {
      mInputPaths.add(path);
      return this;
    }

    /**
     * Replaces the current list of files to load with the set of files
     * specified as an argument.
     *
     * @param paths The input file/directory paths.
     * @return This builder instance.
     */
    public Builder withInputPaths(List<Path> paths) {
      mInputPaths.clear();
      mInputPaths.addAll(paths);
      return this;
    }

    /**
     * Sets a flag indicating the use of the DistributedCache to distribute
     * input files.
     *
     * @param enabled true if the DistributedCache should be used, false otherwise.
     * @return This builder instance.
     */
    public Builder withDistributedCache(boolean enabled) {
      mUseDCache = enabled;
      return this;
    }

    /**
     * Builds and returns a new FileStoreHelper instance.
     *
     * @return a new, configured FileStoreHelper.
     */
    public FileStoreHelper build() {
      return new FileStoreHelper(this);
    }
  }

  /** @return a new FileStoreHelper.Builder instance. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Constructor invoked by Builder.build().
   *
   * @param builder the builder to configure from.
   */
  private FileStoreHelper(Builder builder) {
    mConf = builder.mConf;
    mInputPaths = builder.mInputPaths;
    mUseDCache = builder.mUseDCache;
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
   * An aggregator for use with Lists.foldLeft() that expands a list of paths that
   * may include directories to include only files; directories are expanded to multiple
   * file entries that are the files in this directory.
   */
  @ApiAudience.Private
  private final class DirExpandAggregator extends Lists.Aggregator<Path, List<Path>> {
    // Note: This class could be factored out to assist other places in Kiji where we need
    // to expand a list of files, dirs and globs into just a list of files, if necessary.

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
   * Returns the set of raw input path(s) that were specified for read. This may
   * include wildcards or directories. You should use getExpandedInputPaths()
   * to determine the set of files to actually read.
   *
   * @return a copy of the set of raw input paths specified for read.
   */
  public List<Path> getInputPaths() {
    return Collections.unmodifiableList(new ArrayList<Path>(mInputPaths));
  }

  /**
   * Returns the set of input path(s) that should be actually opened for read.
   * This set of paths may be on local disk (e.g., if the DistributedCache was used
   * to transmit the files), or in HDFS. This will not contain directory names nor
   * globs; it is expanded to the literal set of files to open.
   *
   * <p>Each Path object returned is fully qualified, and represents an absolute
   * path that should be opened by its associated FileSystem object.</p>
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
   * If the cache URI prefix is already set, return this value. Otherwise create
   * a new unique cache URI prefix. This does not memoize its return value;
   * if mDCachePrefix is empty/null, multiple calls to this method will return
   * unique values.
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

  /**
   * Serializes file- and DistributedCache-specific properties associated
   * with the KeyValueStore that owns this FileStoreHelper to the specified configuration.
   *
   * @param conf the configuration to populate.
   * @throws IOException if there's an error serializing the state.
   */
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mInputPaths.isEmpty()) {
      throw new IOException("Required attribute not set: input path");
    }

    if (mUseDCache && !"local".equals(conf.get("mapred.job.tracker", ""))) {
      // If we're scheduled to use the distributed cache, and we're not in the LocalJobRunner,
      // add these files to the DistributedCache.

      // TODO(aaron): This does not handle any sort of MapperTester, etc.
      // We need a separate flag that tells this to ignore mUseDCache if we're in a test
      // environment, and just use the original input file specs.
      final String dCachePrefix = getCachePrefix();

      // Associate this randomly chosen prefix id with this KVStore implementation.
      conf.set(CONF_DCACHE_PREFIX_KEY, dCachePrefix);

      // Add the input paths to the DistributedCache and translate path names.
      int uniqueId = 0;
      // TODO: getExpandedInputPaths() should use the Configuration from conf, not our getConf().
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
      conf.setStrings(CONF_PATHS_KEY,
          Lists.toArray(Lists.map(mInputPaths, new Lists.ToStringFn<Path>()), String.class));
    }
  }

  /**
   * Deserializes file- and DistributedCache-specific properties associated
   * with the KeyValueStore that owns this FileStoreHelper from the specified configuration.
   *
   * <p>This retains a reference to the KeyValueStoreConfiguration's backing Configuration
   * instance to use when opening files specified by this configuration.</p>
   *
   * @param conf the configuration to read.
   * @throws IOException if there's an error deserializing the configuration.
   */
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    setConf(conf.getDelegate());

    mDCachePrefix = conf.get(CONF_DCACHE_PREFIX_KEY, "");
    LOG.debug("Input dCachePrefix: " + mDCachePrefix);
    if (mDCachePrefix.isEmpty()) {
      // Read an ordinary list of files from the Configuration.
      // These may include directories and globs to expand.
      mInputPaths = Lists.map(Arrays.asList(conf.getStrings(
          CONF_PATHS_KEY, new String[0])),
          new Lists.Func<String, Path>() {
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

    // If we are initializing a client-side instance to later serialize, the user may have
    // specified HDFS files, but also an intent to put the files in the DistributedCache. Set
    // this flag now, which will generate mDCachePrefix when addToConfiguration() is called
    // later.
    mUseDCache = conf.getBoolean(CONF_USE_DCACHE_KEY, USE_DCACHE_DEFAULT);
  }
}

