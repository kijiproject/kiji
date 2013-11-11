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

package org.kiji.checkin.models;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.HashCodeBuilder;

import org.kiji.checkin.VersionInfo;

/**
 * <p>Represents a check-in message that can be sent to a Kiji BentoBox upgrade server. A check-in
 * message contains information such as the version of Kiji BentoBox being used,
 * a unique identifier for the user of Kiji BentoBox, and the last time the <code>kiji</code>
 * script included with the distribution was used. Check-in messages are periodically sent to the
 * upgrade server both so it can track usage information and as a means of requesting information
 * on new, available updates.</p>
 *
 * <p>To create a check-in message, use {@link UpgradeCheckin.Builder}. A message can be
 * transformed to JSON by calling {@link #toJSON()}.</p>
 */
public final class UpgradeCheckin implements JsonBeanInterface {
  /** The name of the config file containing the project name. */
  private static final String CONFIG_FILE_NAME = "project-name.properties";

  /** The property key for the project name. This key should be in the config file. */
  private static final String PROJECT_NAME_PROPERTY = "project-name";

  /** The default project name if the config file is not found. */
  private static final String DEFAULT_PROJECT_NAME = "kiji";

  /** The type of this message (used by the upgrade server). */
  @SerializedName("type")
  private final String mType;

  /** The format version of this message. */
  @SerializedName("request_format")
  private final String mFormat;

  /** The operating system of the current user sending this check-in message. */
  @SerializedName("os")
  private final String mOperatingSystem;

  /** The version of BentoBox being used by the user sending this check-in message. */
  @SerializedName("bento_version")
  private final String mBentoVersion;

  /** The Java version being used by the current user sending this check-in message. */
  @SerializedName("java_version")
  private final String mJavaVersion;

  /**
   * Unix time in milliseconds of the last usage of the <code>kiji</code> command by the user
   * sending this check-in message.
   */
  @SerializedName("last_used")
  private final long mLastUsedMillis;

  /** A unique (anonymous) identifer for the user sending this check-in message. */
  @SerializedName("id")
  private final String mId;

  /** The name of the project, "kiji", or "wibi", which differentiates enterprise releases. */
  @SerializedName("project_name")
  private final String mProjectName;

  /**
   * Constructs a new check-in message using values in the specified builder.
   *
   * @param builder to obtain message values from.
   */
  private UpgradeCheckin(Builder builder) {
    mType = builder.mType;
    mFormat = builder.mFormat;
    mOperatingSystem = builder.mOperatingSystem;
    mBentoVersion = builder.mBentoVersion;
    mJavaVersion = builder.mJavaVersion;
    mLastUsedMillis = builder.mLastUsedMillis;
    mId = builder.mId;
    mProjectName = builder.mProjectName;
  }

  /**
   * Creates a JSON serialization of this check-in message.
   *
   * @return a JSON string that is a serialization of this instance.
   */
  public String toJSON() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof UpgradeCheckin)) {
      return false;
    }
    UpgradeCheckin that = (UpgradeCheckin) other;
    return mType.equals(that.getType())
        && mFormat.equals(that.getFormat())
        && mOperatingSystem.equals(that.getOperatingSystem())
        && mBentoVersion.equals(that.getBentoVersion())
        && mJavaVersion.equals(that.getJavaVersion())
        && mLastUsedMillis == that.getLastUsedMillis()
        && mId.equals(that.getId())
        && mProjectName.equals(that.getProjectName());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(mType)
        .append(mFormat)
        .append(mOperatingSystem)
        .append(mBentoVersion)
        .append(mJavaVersion)
        .append(mLastUsedMillis)
        .append(mId)
        .append(mProjectName)
        .toHashCode();
  }

  /**
   * @return the type of this message.
   */
  public String getType() {
    return mType;
  }

  /**
   * @return the format version for this message.
   */
  public String getFormat() {
    return mFormat;
  }

  /**
   * @return the operating system information included with this message.
   */
  public String getOperatingSystem() {
    return mOperatingSystem;
  }

  /**
   * @return the BentoBox version included with this message.
   */
  public String getBentoVersion() {
    return mBentoVersion;
  }

  /**
   * @return the Java version included with this message.
   */
  public String getJavaVersion() {
    return mJavaVersion;
  }

  /**
   * @return the Unix time in milliseconds of the last usage of the <code>kiji</code> script
   *     included with this message.
   */
  public long getLastUsedMillis() {
    return mLastUsedMillis;
  }

  /**
   * @return the user identifier included with this message.
   */
  public String getId() {
    return mId;
  }

  /**
   * @return the project name included with this message.
   */
  public String getProjectName() {
    return mProjectName;
  }

  /**
   * <p>Can be used to build an upgrade check-in message. Clients should use the "with"
   * methods of this builder to configure message parameters, and then call {@link #build()} to
   * obtain an instance of {@link UpgradeCheckin} with content equivalent to the values
   * configured. It is an error to attempt to obtain an instance with {@link #build()} without
   * first supplying all content for the messsage.</p>
   *
   * <p>This builder will only create check-in messages with type <code>checkversion</code> and
   * format <code>bento-checkin-1.0.0</code>. Furthermore, operating system, bento version,
   * and java version information included in the check-in message will be set by this builder
   * using {@link System#getProperties()}. Clients should only need to set the user id and last
   * usage timestamp of the message through the builder to create a new check-in message.</p>
   */
  public static class Builder {
    /** The type of message created by this builder. */
    private static final String MESSAGE_TYPE = "checkversion";

    /** The format of messages created by this builder. */
    private static final String MESSAGE_FORMAT = "bento-checkin-1.0.0";

    /** Type of message built. */
    private String mType;

    /** Format of message being built. */
    private String mFormat;

    /**
     * The usage timestamp for the <code>kiji</code> command to include in the built check-in
     * message.
     */
    private Long mLastUsedMillis = null;

    /** The unique and anonymous user identifier to include with the message. */
    private String mId;

    private String mProjectName;

    /** Operating system identifier. */
    private String mOperatingSystem;

    /** Version of Kiji BentoBox in use. */
    private String mBentoVersion;

    /** Java version in use. */
    private String mJavaVersion;

    /** Class that is used to fetch package name and version info. Caller
     * would pass in a representative class from the jar they wish to use to query package
     * name and version info */
    private Class<?> mVersionInfoClass;

    /**
     * Constructs a new Builder given a class that will be used to query package information
     * from the manifest.
     *
     * @param versionInfoClass representative class from the jar containing the proper manifest
     * information.
     */
    public Builder(Class<?> versionInfoClass) {
      mVersionInfoClass = versionInfoClass;
    }

    /**
     * Configures this builder to create a check-in message using the specified value for the
     * Unix time in milliseconds of the last usage of the <code>kiji</code> script.
     *
     * @param lastUsedMillis is the Unix time, in milliseconds, of the last usage of the
     *     <code>kiji</code> script included with BentoBox.
     * @return this instance, so configuration can be chained.
     */
    public Builder withLastUsedMillis(Long lastUsedMillis) {
      mLastUsedMillis = lastUsedMillis;
      return this;
    }

    /**
     * Configures this builder to create a check-in message using the specified unique and
     * anonymous identifier for the user.
     *
     * @param id that is unique and anonymous for the client sending the built message.
     * @return this instance, so configuration can be chained.
     */
    public Builder withId(String id) {
      mId = id;
      return this;
    }

    /**
     * Gets the value for the system property with the specified key. If querying for the system
     * property returns <code>null</code>, this method will throw an
     * {@link IllegalArgumentException}. This method may also throw the same runtime exceptions
     * thrown by {@link System#getProperty(String)}.
     *
     * @param key to look-up in system properties.
     * @return the value of the system property.
     */
    private String getSystemProperty(String key) {
      String value = System.getProperty(key);
      if (null == value) {
        throw new IllegalArgumentException("There was no value for system property: " + key);
      }
      return value;
    }

    /**
     * Uses system properties to get the operating system name, version,
     * and architecture formatted into a single string.
     *
     * @return a string containing operating system name, version, and architecture.
     */
    private String getOperatingSystem() {
      String osName = getSystemProperty("os.name");
      String osVersion = getSystemProperty("os.version");
      String osArchitecture = getSystemProperty("os.arch");
      return String.format("%s %s %s", osName, osVersion, osArchitecture);
    }

    /**
     * Gets the project name for this project from the expected configuration file.  Returns "kiji"
     * if the expected config file does not exist.
     *
     * @return the project name for this project from the expected configuration file.  Returns
     *     "kiji" if the expected config file does not exist.
     */
    private String getProjectName() {
      Properties prop = new Properties();
      try {
        InputStream resource = Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(CONFIG_FILE_NAME);
        if (resource == null) {
          return DEFAULT_PROJECT_NAME;
        } else {
          prop.load(Thread
              .currentThread()
              .getContextClassLoader()
              .getResourceAsStream(CONFIG_FILE_NAME));
          return prop.getProperty(PROJECT_NAME_PROPERTY);
        }
      } catch (IOException e) {
        return DEFAULT_PROJECT_NAME;
      }
    }

    /**
     * Creates a new check-in message using values configured through this builder and the values
     * of system properties.
     *
     * @return a new check-in message with content configured through this builder and system
     *     properties.
     * @throws IOException if there is a problem retrieving the current version of BentoBox in use.
     */
    public UpgradeCheckin build()
        throws IOException {
      // Ensure the client has completely configured the builder.
      checkNotNull(mId, "User id not supplied to checkin message builder.");
      checkNotNull(mLastUsedMillis, "Last usage timestamp for kiji script not supplied to "
          + "check-in message builder.");
      // Get other message parameters using system properties.
      mType = MESSAGE_TYPE;
      mFormat = MESSAGE_FORMAT;
      mOperatingSystem = getOperatingSystem();
      mProjectName = getProjectName();
      mBentoVersion = VersionInfo.getSoftwareVersion(mVersionInfoClass);
      mJavaVersion = getSystemProperty("java.version");
      return new UpgradeCheckin(this);
    }
  }
}
