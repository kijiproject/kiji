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
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import org.apache.commons.lang.builder.HashCodeBuilder;

import org.kiji.checkin.UUIDTools;
import org.kiji.checkin.VersionInfo;

/**
 * <p>
 * Represents a message that can be sent to a Kiji BentoBox upgrade server. A check-in message
 * contains information such as the version of Kiji BentoBox being used, a unique identifier for the
 * user of Kiji BentoBox, and the last time the <code>kiji</code> script included with the
 * distribution was used. messages are periodically sent to the upgrade server both so it can track
 * usage information and as a means of requesting information on new, available updates.
 * </p>
 *
 * <p>
 * To create a message, use {@link KijiCommand.Builder}. A message can be transformed to JSON by
 * calling {@link #toJSON()}.
 * </p>
 */
public final class KijiCommand implements JsonBeanInterface {

  /** The type of this message (used by the upgrade server). */
  @SerializedName("type")
  private final String mType;

  /** Whether or not the command execution was a success or not. */
  @SerializedName("success")
  private final boolean mSuccess;

  /** The actual command string to log. */
  @SerializedName("command")
  private final String mCommandName;

  /** The operating system of the current user sending this message. */
  @SerializedName("os")
  private final String mOperatingSystem;

  /** The version of package being used by the user sending this message. */
  @SerializedName("package_version")
  private final String mPackageVersion;

  /** The name of package being used by the user sending this message. */
  @SerializedName("package_name")
  private final String mPackageName;

  /** The Java version being used by the current user sending this message. */
  @SerializedName("java_version")
  private final String mJavaVersion;

  /** A unique (anonymous) identifer for the user sending this message. */
  @SerializedName("id")
  private final String mId;

  /**
   * Constructs a new message using values in the specified builder.
   *
   * @param builder to obtain message values from.
   */
  private KijiCommand(Builder builder) {
    mType = builder.mType;
    mPackageName = builder.mPackageName;
    mOperatingSystem = builder.mOperatingSystem;
    mPackageVersion = builder.mPackageVersion;
    mJavaVersion = builder.mJavaVersion;
    mCommandName = builder.mCommandName;
    mSuccess = builder.mSuccess;
    mId = builder.mId;
  }

  /**
   * Creates a JSON serialization of this message.
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
    if (!(other instanceof KijiCommand)) {
      return false;
    }
    KijiCommand that = (KijiCommand) other;
    return mType.equals(that.getType()) && mOperatingSystem.equals(that.getOperatingSystem())
        && mPackageVersion.equals(that.getPackageVersion())
        && mPackageName.equals(that.getPackageName()) && mJavaVersion.equals(that.getJavaVersion())
        && mId.equals(that.getId());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(mType).append(mOperatingSystem).append(mPackageVersion)
        .append(mJavaVersion).append(mId).toHashCode();
  }

  /**
   * @return the type of this message.
   */
  public String getType() {
    return mType;
  }

  /**
   * @return the operating system information included with this message.
   */
  public String getOperatingSystem() {
    return mOperatingSystem;
  }

  /**
   * @return the package version included with this message.
   */
  public String getPackageVersion() {
    return mPackageVersion;
  }

  /**
   * @return the package name included with this message.
   */
  public String getPackageName() {
    return mPackageName;
  }

  /**
   * @return the Java version included with this message.
   */
  public String getJavaVersion() {
    return mJavaVersion;
  }

  /**
   * @return the user identifier included with this message.
   */
  public String getId() {
    return mId;
  }

  /**
   * <p>
   * Can be used to build an upgrade message. Clients should use the "with" methods of this builder
   * to configure message parameters, and then call {@link #build()} to obtain an instance of
   * {@link KijiCommand} with content equivalent to the values configured. It is an error to attempt
   * to obtain an instance with {@link #build()} without first supplying all content for the
   * message.
   * </p>
   *
   * <p>
   * This builder will only create messages with type <code>postcommand</code>. Furthermore,
   * operating system and java version information included in the message will be set by this
   * builder using {@link System#getProperties()}. Clients should only need to set the user id,
   * package name, and package version through the builder to create a new message.
   * </p>
   */
  public static class Builder {
    /** The type of message created by this builder. */
    private static final String MESSAGE_TYPE = "postcommand";

    /** Whether or not the command execution was a success or not. */
    private boolean mSuccess;

    /** Type of message built. */
    private String mType;

    /** The unique and anonymous user identifier to include with the message. */
    private String mId;

    /** Operating system identifier. */
    private String mOperatingSystem;

    /** Version of Kiji BentoBox in use. */
    private String mPackageVersion;

    /** Name of Kiji Package in use. */
    private String mPackageName;

    /** Java version in use. */
    private String mJavaVersion;

    /**
     * Class that is used to fetch package name and version info. Caller
     * would pass in a representative class from the jar they wish to use to query package
     * name and version info
     */
    private final Class<?> mVersionInfoClass;

    /** The command name used. */
    private String mCommandName;

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
     * Configures this builder to create a message using the command name/string
     * issued to kiji.
     *
     * @param commandName is the name of the command issued.
     * @return this instance, so configuration can be chained.
     */
    public Builder withCommandName(String commandName) {
      mCommandName = commandName;
      return this;
    }

    /**
     * Configures this builder to create a message using the command name/string
     * issued to kiji.
     *
     * @param success is whether or not the command executed successfully.
     * @return this instance, so configuration can be chained.
     */
    public Builder withSuccess(boolean success) {
      mSuccess = success;
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
     * Creates a new message using values configured through this builder and the values
     * of system properties.
     *
     * @return a new message with content configured through this builder and system
     *         properties.
     * @throws IOException if there is a problem retrieving the current version of BentoBox in use.
     */
    public KijiCommand build() throws IOException {
      // Ensure the client has completely configured the builder.
      checkNotNull(mCommandName, "Command not supplied to message builder.");
      // Get other message parameters using system properties.
      mType = MESSAGE_TYPE;
      mOperatingSystem = getOperatingSystem();
      mPackageName = VersionInfo.getSoftwarePackage(mVersionInfoClass);
      mPackageVersion = VersionInfo.getSoftwareVersion(mVersionInfoClass);
      mJavaVersion = getSystemProperty("java.version");

      // Get the saved upgrade response, or fail if there is none.
      String uuid = UUIDTools.getOrCreateUserUUID();
      if (null == uuid) {
        // We couldn't save the UUID to the user's home dir for some reason. That's
        // not a reason to refuse to upgrade though. Create a 1-off uuid and use that.
        uuid = UUID.randomUUID().toString();
        assert null != uuid;
      }

      mId = uuid;
      return new KijiCommand(this);
    }
  }
}
