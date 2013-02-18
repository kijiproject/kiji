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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.framework.KeyValueStoreConfiguration;

/**
 * KeyValueStore implementation that reads delimited records from text files.
 *
 * <p>Each line of an input text file is made available as a record in the KeyValueStore.
 * Lines are separated into keys and values by the first instance of the <i>delimiter</i> string.
 * The second (and additional) instances of the delimiter string in the line are part of the
 * value, and are not treated specially.</p>
 *
 * <p>Input lines are read through BufferedReader.readLine(). A line is considered to be
 * terminated by any of a line feed ('\n'), carriage return ('\r'), or a carriage return
 * followed immediately by a linefeed.</p>
 *
 * <p>Line termination characters are removed before data is put in the KeyValueStore.</p>
 *
 * <p>By default, keys and values are tab-delimited. The delimiter may be set by the
 * <tt>setDelimiter()</tt> method of the TextFileKeyValueStore.Builder instance.
 * The delimiter may be multiple characters long.
 * The following rules are applied when breaking lines into keys and values:</p>
 * <ul>
 *   <li>Keys and values are separated by the first instance of the delimiter in the line.
 *       Further instances of the delimiter string are retained as part of the value.</li>
 *   <li>A key followed immediately by a line terminator has a <tt>null</tt> value associated
 *       with it. <tt>containsKey()</tt> will return <tt>true</tt> for this key, and
 *       <tt>get()</tt> will return <tt>null</tt>.</li>
 *   <li>A key followed by the delimiter and then the line terminator has the empty string
 *       (<tt>&quot;&quot;</tt>) as its value.</li>
 *   <li>A line that begins with a delimiter has the empty string as its key; the remainder
 *       of the line following the delimiter is the value.</li>
 *   <li><tt>null</tt> is never allowed or possible as a key.</li>
 *   <li>A blank line uses the above rules as follows: the key is the empty string
 *       (<tt>&quot;&quot;</tt>), and the value is <tt>null</tt>.</li>
 *   <li>The last line in the file does not need to be newline-terminated; this is treated
 *       like any other line in the file.</li>
 * </ul>
 *
 * <p>A kvstores XML file may contain the following properties when specifying the
 * behavior of this class:</p>
 * <ul>
 *   <li><tt>dcache</tt> - True if files should be accessed by jobs through the DistributedCache.
 *   <li><tt>delim</tt> - The delimiter string that separates keys and values within
 *       a line of text. (Default is a tab character.)</li>
 *   <li><tt>paths</tt> - A comma-separated list of HDFS paths to files backing this store.
 * </ul>
 */
@ApiAudience.Public
public final class TextFileKeyValueStore implements KeyValueStore<String, String> {

  /** The configuration variable for the delimiter. */
  private static final String CONF_DELIMITER_KEY = "delim";

  /**
   * Records are tab-delimited by default, to be compatible with TextOutputFormat,
   * and function like KeyValueTextInputFormat.
   */
  public static final String DEFAULT_DELIMITER = "\t";

  /** Helper object to manage backing files. */
  private final FileStoreHelper mFileHelper;

  /** The delimiter string to use. */
  private String mDelim;

  /** true if the user has called open(); cannot call initFromConf() after that. */
  private boolean mOpened;

  /**
   * A Builder-pattern class that configures and creates new TextFileKeyValueStore
   * instances. You should use this to specify the input to this KeyValueStore.
   * Call the build() method to return a new, configured TextFileKeyValueStore instance.
   */
  @ApiAudience.Public
  public static final class Builder {
    private FileStoreHelper.Builder mFileBuilder;
    private String mDelim;

    /**
     * Private, default constructor. Call the builder() method of this KeyValueStore
     * to get a new builder instance.
     */
    private Builder() {
      mFileBuilder = FileStoreHelper.builder();
      mDelim = DEFAULT_DELIMITER;
    }

    /**
     * Specifies the delimiter between the key and the value on a line in the file.
     *
     * @param delim the delimiter string to use.
     * @return this Builder instance.
     */
    public Builder withDelimiter(String delim) {
      if (null == delim || delim.isEmpty()) {
        throw new IllegalArgumentException("Cannot use empty delimiter");
      }
      mDelim = delim;
      return this;
    }

    /**
     * Sets the Hadoop configuration instance to use.
     *
     * @param conf The configuration.
     * @return This builder instance.
     */
    public Builder withConfiguration(Configuration conf) {
      mFileBuilder.withConfiguration(conf);
      return this;
    }

    /**
     * Adds a path to the list of files to load.
     *
     * @param path The input file/directory path.
     * @return This builder instance.
     */
    public Builder withInputPath(Path path) {
      mFileBuilder.withInputPath(path);
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
      mFileBuilder.withInputPaths(paths);
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
      mFileBuilder.withDistributedCache(enabled);
      return this;
    }

    /**
     * Build a new TextFileKeyValueStore instance.
     *
     * @return the initialized KeyValueStore.
     */
    public TextFileKeyValueStore build() {
      return new TextFileKeyValueStore(this);
    }
  }

  /**
   * Creates a new TextFileKeyValueStore.Builder instance that can be used
   * to configure and create a new KeyValueStore.
   *
   * @return a new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reflection-only constructor. Used only for reflection. You should create and configure
   * TextFileKeyValueStore instances by using a builder; call TextFileKeyValueStore.builder()
   * to get a new builder instance.
   */
  public TextFileKeyValueStore() {
    this(builder());
  }

  /**
   * Main constructor used by the builder; creates a new TextFileKeyValueStore to read text files.
   *
   * @param builder the builder to configure from.
   */
  private TextFileKeyValueStore(Builder builder) {
    mFileHelper = builder.mFileBuilder.build();
    mDelim = builder.mDelim;
    mOpened = false;
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    if (null == mDelim || mDelim.isEmpty()) {
      throw new IOException("Cannot use empty delimiter");
    }

    conf.set(CONF_DELIMITER_KEY, mDelim);
    mFileHelper.storeToConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    if (mOpened) {
      throw new IllegalStateException("Cannot reinitialize; already opened a reader.");
    }

    mFileHelper.initFromConf(conf);
    mDelim = conf.get(CONF_DELIMITER_KEY, DEFAULT_DELIMITER);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<String, String> open() throws IOException {
    mOpened = true;
    return new Reader(mFileHelper.getConf(), mFileHelper.getExpandedInputPaths(), mDelim);
  }

  /**
   * Reads an entire text file of records into memory, and indexes it by the key field.
   * Key and value fields are separated by the first occurrence of the 'delimiter' string
   * in a line. If the delimiter does not exist, then the entire line is taken to be the
   * key and the value is 'null'. (Note that lines that end with the delimiter string
   * will have the non-null empty string as a value.)
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>. Where multiple files back the reader, the order in which files
   * are processed is undefined.</p>
   */
  @ApiAudience.Private
  private static final class Reader implements KeyValueStoreReader<String, String> {
    /** A map from keys to values loaded from the input files. */
    private Map<String, String> mMap;

    /** The delimiter string. */
    private final String mDelim;

    /**
     * Constructs a key value reader over text file(s).
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the text file(s).
     * @param delim the delimeter string.
     * @throws IOException If the files cannot be read.
     */
    public Reader(Configuration conf, List<Path> paths, String delim) throws IOException {
      if (null == delim || delim.isEmpty()) {
        throw new IOException("Cannot use null/empty delimiter.");
      }

      mDelim = delim;
      mMap = new HashMap<String, String>();

      for (Path path : paths) {
        // Load the entire file into the lookup map.
        FileSystem fs = path.getFileSystem(conf);
        BufferedReader reader = null;
        try {
          reader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
          String line = reader.readLine();
          while (null != line) {
            String key;
            String val;
            int delimPos = line.indexOf(mDelim);
            if (-1 == delimPos) {
              // No delimiter in the line.
              key = line; // Whole line is the key.
              val = null; // The value is null.
            } else {
              key = line.substring(0, delimPos);
              val = line.substring(delimPos + mDelim.length());
            }

            if (!mMap.containsKey(key)) {
              mMap.put(key, val);
            }

            line = reader.readLine();
          }
        } catch (UnsupportedEncodingException uee) {
          // Can't open as UTF-8? Java is quite broken if we get here
          throw new IOException(uee);
        } finally {
          IOUtils.closeQuietly(reader);
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return null != mMap;
    }

    /** {@inheritDoc} */
    @Override
    public String get(String key) throws IOException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(String key) throws IOException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mMap = null;
    }
  }
}
