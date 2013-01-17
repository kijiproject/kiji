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

package org.kiji.mapreduce.tools;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

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
public class JobOutputSpec {
  /**
   * The job output formats supported by Kiji.  In the string
   * representation of a JobOutputSpec, this is the part before the
   * first colon, e.g., the "avro" in "avro:/path/to/avro/container/file@8"
   */
  public static enum Format {
    /** A Kiji table. */
    KIJI("kiji", false),
    /** Text files in a file system. */
    TEXT("text", true),
    /** Sequence files in a file system. */
    SEQUENCE_FILE("seq", true),
    /** Map files in a file system. */
    MAP_FILE("map", true),
    /** Avro container files in a file system. */
    AVRO("avro", true),
    /** Avro container files of key/value generic records. */
    AVRO_KV("avrokv", true),
    /** HFiles used in HBase for bulk loading into region servers. */
    HFILE("hfile", true);

    /** The short name of the format. */
    private String mName;
    /** Whether a location is required by this format. */
    private boolean mIsLocationRequired;

    /** A static map from a format name to formats. */
    private static Map<String, Format> mNameMap;
    static {
      // Initialize the map from names to Formats for quick lookup later.
      mNameMap = new HashMap<String, Format>();
      for (Format format : Format.class.getEnumConstants()) {
        mNameMap.put(format.getName(), format);
      }
    }

    /**
     * Constructor for a member of the Format enum.
     *
     * @param name The short name of the format.
     * @param isLocationRequired Whether the format requires a location.
     */
    private Format(String name, boolean isLocationRequired) {
      mName = name;
      mIsLocationRequired = isLocationRequired;
    }

    /**
     * The short name of the format.
     *
     * @return The format name.
     */
    public String getName() {
      return mName;
    }

    /** @return Whether a location is required by the format. */
    public boolean isLocationRequired() {
      return mIsLocationRequired;
    }

    /**
     * Gets a Format object from a short name.
     *
     * @param name The short name of the format.
     * @return The Format object.
     * @throws JobIOSpecParseException If the name does not identify a valid format.
     */
    public static Format parse(String name) throws JobIOSpecParseException {
      Format format = mNameMap.get(name);
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
  public JobOutputSpec(Format format, String location, int splits) {
    mFormat = format;
    mLocation = location;
    mSplits = splits;
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

  /**
   * Parses the string representation of a JobOutputSpec.  The string
   * representation is of the format {@literal "<format>:<location>@<splits>"}.
   *
   * @param spec The output spec string to parse.
   * @return The parsed JobOutputSpec.
   * @throws JobIOSpecParseException If it is unable to parse.
   */
  public static JobOutputSpec parse(String spec) throws JobIOSpecParseException {
    String[] parts = StringUtils.split(spec, ":", 2);

    // Parse the format.
    Format format = Format.parse(parts[0]);

    // Parse the location and splits only if it was required.
    String location = null;
    int splits = 0;
    if (format.isLocationRequired()) {
      if (parts.length != 2) {
        throw new JobIOSpecParseException("Location required", spec);
      }
      String[] fields = StringUtils.split(parts[1], "@");
      if (fields.length != 2) {
        throw new JobIOSpecParseException("Number of splits required", spec);
      }
      location = fields[0];
      splits = Integer.parseInt(fields[1]);
    }

    return new JobOutputSpec(format, location, splits);
  }
}
