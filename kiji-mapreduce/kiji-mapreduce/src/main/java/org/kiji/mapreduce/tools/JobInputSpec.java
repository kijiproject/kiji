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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.kiji.annotations.ApiAudience;

/**
 * <p>Describes the format and location of the input for a MapReduce job.</p>
 *
 * <p>The input for a MapReduce job can be described with two components: format and
 * location.  The format determines how the input data should be read, e.g., is it a text
 * file or a sequence file?  The location may depend on the format.  For a file, it is
 * usually a URL or filesystem path.  For a Kiji table, it is simply the name of the
 * table.</p>
 *
 * <p>JobInputSpecs can be constructed if you know the format and location, or they can be
 * parsed from a String using the {@link JobInputSpec#parse(String)}
 * method.</p>
 */
@ApiAudience.Framework
public class JobInputSpec {
  /**
   * The Job input formats supported by Kiji.  In the string representation of a
   * JobInputSpec, this is the part before the first colon, e.g. the "avro" in
   * "avro:/path/to/avro/container/file."
   */
  public static enum Format {
    /** Avro container files from a file system. */
    AVRO("avro"),
    /** Avro container files of key/value generic records. */
    AVRO_KV("avrokv"),
    /** An HBase table. */
    HTABLE("htable"),
    /** Sequence files from a file system. */
    SEQUENCE("seq"),
    /** Single files in hdfs get read as one record per file. */
    SMALL_TEXT_FILES("small-text-files"),
    /** Text files from a file system. */
    TEXT("text"),
    /** A Kiji table. */
    KIJI("kiji");

    /** The short name of the format. */
    private String mName;

    /** A static map from short names to formats. */
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
     */
    private Format(String name) {
      mName = name;
    }

    /**
     * The short name of the format (used as the prefix in the string
     * representation of the JobInputSpec).
     *
     * @return The name of the format.
     */
    public String getName() {
      return mName;
    }

    /**
     * Return the format referred to by the name.
     *
     * @param name The short name of the format.
     * @return the Format object.
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

  /** The format of the MapReduce job input. */
  private Format mFormat;
  /**
   * The location of the MapReduce job input, whose meaning may depend
   * on the format (usually a filesystem path).
   * May specify multiple inputs as long as they are all of the same type.
   */
  private String[] mLocations;

  /**
   * Constructor.  The KIJI and HTABLE formats must specify exactly one input location.
   * All other formats may specify multiple input locations.
   *
   * @param format The format of the input data.
   * @param locations The locations of the input data.
   */
  public JobInputSpec(Format format, String... locations) {
    if ((Format.KIJI == format || Format.HTABLE == format) && locations.length != 1) {
      throw new UnsupportedOperationException("Format " + format.toString()
          + " only supports a single input location."
          + "  You specified: " + Arrays.toString(locations));
    }
    mFormat = format;
    mLocations = locations;
  }

  /** @return The format of the input data. */
  public Format getFormat() {
    return mFormat;
  }

  /**
   * Convenience method to return the location iff exactly one is specified.
   * Throws an IllegalStateException if not.
   *
   * @return The location of the input data.
   */
  public String getLocation() {
    if (null == mLocations || 1 != mLocations.length) {
      throw new IllegalStateException("getLocation() may only be used if there is exactly"
          + " one location specified.  Locations are: " + Arrays.toString(getLocations()));
    }
    return mLocations[0];
  }

  /** @return The locations of the input data. */
  public String[] getLocations() {
    return mLocations.clone();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return mFormat.getName() + ":" + Arrays.toString(mLocations);
  }

  /**
   * <p>Parses the string representation of a JobInputSpec.</p>
   *
   * <p>JobInputSpecs are of the form {@literal "<format>:<location>"}.</p>
   *
   * @param spec The input spec string to parse.
   * @return The parsed JobInputSpec.
   * @throws JobIOSpecParseException If is unable to parse.
   */
  public static JobInputSpec parse(String spec) throws JobIOSpecParseException {
    // Split it on ':'.
    String[] parts = StringUtils.split(spec, ":", 2);
    if (parts.length != 2) {
      throw new JobIOSpecParseException("Should be '<format>:<location>'", spec);
    }

    // Parse the format.
    Format format = Format.parse(parts[0]);

    // Parse the location.
    String commaDelimitedLocations = parts[1];
    String[] locations = StringUtils.split(commaDelimitedLocations, ",");
    return new JobInputSpec(format, locations);
  }
}
