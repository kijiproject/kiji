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

package org.kiji.mapreduce.tools.framework;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;

/**
 * Describes the format and location for the output of a MapReduce job.
 *
 * <p>The output for a MapReduce job can be described with three components: format,
 * location, and a number of splits. The format determines how the output data should be
 * written, e.g., as a text file or sequence file?  The location may depend on the
 * format.  For a file, it is usually a URL or filesystem path.  For a Kiji table, it is
 * simply the name of the table.  Finally, the number of splits is used to determine the
 * number or reducers.  When writing files, the number of sharded output files will
 * be equal to the number of reducers, since each reducer writes one shard.</p>
 */
@ApiAudience.Framework
public final class JobOutputSpec {
  /**
   * The job output formats supported by Kiji.  In the string
   * representation of a JobOutputSpec, this is the part before the
   * first colon, e.g., the "avro" in "avro:/path/to/avro/container/file@8"
   */
  public static enum Format {
    /** A Kiji table. */
    KIJI("kiji"),
    /** Text files in a file system. */
    TEXT("text"),
    /** Sequence files in a file system. */
    SEQUENCE_FILE("seq"),
    /** Map files in a file system. */
    MAP_FILE("map"),
    /** Avro container files in a file system. */
    AVRO("avro"),
    /** Avro container files of key/value generic records. */
    AVRO_KV("avrokv"),
    /** HFiles used in HBase for bulk loading into region servers. */
    HFILE("hfile");

    /** The short name of the format. */
    private String mName;

    /** A static map from a format name to formats. */
    private static final Map<String, Format> NAME_MAP = Maps.newHashMap();
    static {
      // Initialize the map from names to Formats for quick lookup later.
      for (Format format : Format.class.getEnumConstants()) {
        NAME_MAP.put(format.getName(), format);
      }
    }

    /**
     * Initializes a format enum value.
     *
     * @param name Name of the format.
     */
    private Format(String name) {
      mName = name;
    }

    /**
     * Name of the format.
     *
     * @return the format name.
     */
    public String getName() {
      return mName;
    }

    /**
     * Gets a Format object from its name.
     *
     * @param name Name of the format.
     * @return the parsed format enum value.
     * @throws JobIOSpecParseException If the name does not identify a valid format.
     */
    public static Format parse(String name) throws JobIOSpecParseException {
      final Format format = NAME_MAP.get(name);
      if (null == format) {
        throw new JobIOSpecParseException("Unrecognized format", name);
      }
      return format;
    }
  }

  /** The format of the job output data. */
  private Format mFormat;

  /** The location of the job output data, or null if not specified. */
  private String mLocation;

  /**
   * The number of splits for the output data, which determines the
   * number of reducers and the number of sharded output files if this
   * writes to a file system.
   */
  private int mSplits;

  /**
   * Constructor.
   *
   * @param format The job output data format.
   * @param location The target location of the output data (or null
   *     if a location is implied by the format).
   * @param splits The number of output splits.
   */
  private JobOutputSpec(Format format, String location, int splits) {
    mFormat = Preconditions.checkNotNull(format);
    mLocation = Preconditions.checkNotNull(location);
    mSplits = splits;
    Preconditions.checkArgument(splits >= 0);
  }

  /**
   * Creates a new job output specification.
   *
   * @param format is the format of the data output by the job.
   * @param location is the target location of the output data (or <code>null</code> if a location
   *     is implied by the format).
   * @param splits is the number of desired output splits.
   * @return a new job output specification using the specified format, location, and splits.
   */
  public static JobOutputSpec create(Format format, String location, int splits) {
    return new JobOutputSpec(format, location, splits);
  }

  /** @return The format of the output data. */
  public Format getFormat() {
    return mFormat;
  }

  /** @return The target location of the output data (may be null). */
  public String getLocation() {
    return mLocation;
  }

  /** @return The number of splits in the output data. */
  public int getSplits() {
    return mSplits;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return mFormat.getName() + ":" + mLocation + "@" + mSplits;
  }

  /** Regex matching "format:location@split". */
  private static final Pattern RE_JOB_OUTPUT_SPEC = Pattern.compile("([^:]+):(.*)@(\\d+)");

  /**
   * Parses the string representation of a JobOutputSpec.  The string
   * representation is of the format {@literal "<format>:<location>@<splits>"}.
   *
   * @param spec The output spec string to parse.
   * @return The parsed JobOutputSpec.
   * @throws JobIOSpecParseException If it is unable to parse.
   */
  public static JobOutputSpec parse(String spec) throws JobIOSpecParseException {
    final Matcher matcher = RE_JOB_OUTPUT_SPEC.matcher(spec);
    if (!matcher.matches()) {
      throw new JobIOSpecParseException(
          "Invalid job output spec, expecting 'format:location@nsplit'.", spec);
    }
    final Format format = Format.parse(matcher.group(1));
    final String location = matcher.group(2);
    final int nsplits = Integer.parseInt(matcher.group(3));
    return new JobOutputSpec(format, location, nsplits);
  }
}
