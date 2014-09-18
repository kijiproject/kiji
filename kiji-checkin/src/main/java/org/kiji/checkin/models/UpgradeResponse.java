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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import org.kiji.checkin.CheckinUtils;

/**
 * <p>Represents a response from a Kiji BentoBox upgrade server providing information on new
 * versions available for upgrade. In addition to the content returned by the upgrade server,
 * instances of this class can track a "reminder time," which is the last time a Kiji BentoBox
 * user was reminded of the existence of an upgrade.</p>
 *
 * <p>Instances of this class can be obtained using a JSON record (such as when a response from the
 * upgrade server is obtained), or a file containing a JSON record (such as when the local kiji
 * script wishes to inform the user of an upgrade). The methods {@link #fromJSON(String)} and
 * {@link #fromFile(java.io.File)} can be used to get instances in these ways.</p>
 */
public final class UpgradeResponse {

  /** A pattern for matching against and decomposing version strings. */
  private static final Pattern VERSION_PATTERN =
      Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(-rc\\d+)?(-SNAPSHOT)?");

  /** A message that can be formatted that will inform the user of a compatible upgrade. */
  private static final String COMPATIBLE_UPGRADE_MSG_FORMAT = "Version %s of Kiji BentoBox "
      + "(compatible with your version %s) is available for download at %s. %s%n"
      + "Run 'bento upgrade' to automatically upgrade to this version in place.";

  /** A message that can be formatted that will inform the user of an incompatible upgrade. */
  private static final String INCOMPATIBLE_UPGRADE_MSG_FORMAT = "Version %s of Kiji BentoBox "
      + "(incompatible with your version %s) is available for download at %s. %s";

  /** The format version of a response from the upgrade server. */
  @SerializedName("response_format")
  private final String mFormat;

  /**
   * The latest version available for upgrade, which may be incompatible with the current
   * version.
   */
  @SerializedName("latest_version")
  private final String mLatestVersion;

  /** A URL where the latest version available for upgrade can be downloaded. */
  @SerializedName("latest_version_url")
  private final URL mLatestVersionURL;

  /**
   * The latest version available for upgrade which is compatible with the current version. May
   * be equal to the latest version.
   */
  @SerializedName("compatible_version")
  private final String mCompatibleVersion;

  /** A URL where the latest compatible version available for upgrade can be downloaded. */
  @SerializedName("compatible_version_url")
  private final URL mCompatibleVersionURL;

  /** A message sent by the upgrade server about the upgrades available. */
  @SerializedName("msg")
  private final String mMessage;

  /** The UNIX time the user was last reminded about this upgrade. */
  @SerializedName("last_reminder")
  private long mLastReminderMillis;

  /**
   * Constructs a new instance with <code>null</code> values for all fields,
   * and a value of <code>0</code> for the last reminder time. Intended for use with the GSON
   * framework when deserializing instances.
   */
  private UpgradeResponse() {
    mFormat = null;
    mLatestVersion = null;
    mLatestVersionURL = null;
    mCompatibleVersion = null;
    mCompatibleVersionURL = null;
    mMessage = null;
    mLastReminderMillis = 0L;
  }

  /**
   * Creates an upgrade response using a JSON serialization found in the specified file.
   *
   * @param file contains the JSON serialization of an upgrade response.
   * @return a new upgrade response containing the information found in the JSON serialization.
   * @throws IOException if there is a problem reading the file.
   */
  public static UpgradeResponse fromFile(File file) throws IOException {
    // Read the upgrade information JSON from a file and parse.
    String upgradeJson = CheckinUtils.readFileAsString(file);
    return fromJSON(upgradeJson);
  }

  /**
   * Creates an upgrade response using a JSON serialization.
   *
   * @param upgradeJson is the JSON serialization of an upgrade response.
   * @return a new upgrade response containing the information found in the JSON serialization.
   */
  public static UpgradeResponse fromJSON(String upgradeJson) {
    Gson gson = new Gson();
    return gson.fromJson(upgradeJson, UpgradeResponse.class);
  }

  /**
   * Writes a JSON serialization of this upgrade response to the specified file.
   *
   * @param file to which the upgrade response JSON serialization will be written.
   * @throws IOException if there is a problem writing the file.
   */
  public void write(File file) throws IOException {
    Gson gson = new Gson();
    String upgradeJson = gson.toJson(this);
    CheckinUtils.writeObjectToFile(file, upgradeJson);
  }

  /**
   * Sets the last reminder time to the current time. Should be called when the user is reminded
   * of this upgrade.
   */
  public void justReminded() {
    mLastReminderMillis = System.currentTimeMillis();
  }

  /**
   * Determines if the specified amount of time has past since the user was last reminded of this
   * upgrade.
   *
   * @param upgradePeriodMillis is the desired minimum time between upgrade reminders.
   * @return <code>true</code> if its time to remind the user about this upgrade,
   *     <code>false</code> otherwise.
   */
  public boolean isTimeToRemind(long upgradePeriodMillis) {
    long timePastSinceReminder = System.currentTimeMillis() - mLastReminderMillis;
    return timePastSinceReminder >= upgradePeriodMillis;
  }

  /**
   * Parses a version string into its numeric components. This method can handle versions of the
   * form <code>n1.n2.n3</code> where the <code>ni</code> are integers. It can also handle
   * release candidate versions suffixed with <code>-rc</code> and another integer.
   *
   * @param version to parse.
   * @return an array of numeric components in the version, as they appear from left-to-right in
   *     the version string. The array will be of length 3 if the version string does not include
   *     an rc component, and 4 if it does.
   */
  int[] parseVersionString(String version) {
    Matcher versionMatcher = VERSION_PATTERN.matcher(version);
    if (!versionMatcher.matches()) {
      throw new IllegalArgumentException("Supplied version string does not match version string "
          + "pattern: " + version);
    }

    // If the last group (number 4) is null, then we don't have an rc version and there are 3
    // components, otherwise this is an rc version and we have 4 components.
    int groupCount = null == versionMatcher.group(4) ? 3 : 4;
    int[] components = new int[groupCount];
    // Convert each matched group into an integer.
    for (int i = 1; i <= groupCount; i++) {
      String matchGroup = versionMatcher.group(i);
      // If this is the 4th match group, we've matched a -rc part of the version string, and we
      // should get rid of the rc part.
      if (4 == i) {
        matchGroup = matchGroup.replace("-rc", "");
      }
      components[i - 1] = Integer.parseInt(matchGroup);
    }
    return components;
  }

  /**
   * Determines if the first version specified is greater than, less than,
   * or equal to the second version specified, up to but not including rc components.
   *
   * @param firstVersion to use in the comparison.
   * @param secondVersion to use in the comparison.
   * @return <code>-1</code> if the first version is less than the second,
   *     up to RC components, <code>1</code> if the first version is greather than the second
   *     up to RC components, and <code>0</code> if the two versions are equal up to RC components.
   */
  private int compareNonRCVersions(int[] firstVersion, int[] secondVersion) {
    for (int i = 0; i < 3; i++) {
      if (firstVersion[i] < secondVersion[i]) {
        return -1;
      } else if (firstVersion[i] > secondVersion[i]) {
        return 1;
      }
    }
    // No difference in components, they are equal.
    return 0;
  }

  /**
   * Determines if one version is newer than another version.
   *
   * @param firstVersion to use in the comparison.
   * @param secondVersion to use in the comparison.
   * @return <code>true</code> if the <code>firstVersion</code> is newer than the
   *     <code>secondVersion</code>, <code>false</code> otherwise.
   */
  boolean isVersionNewer(String firstVersion, String secondVersion) {
    int[] firstVersionComponents = parseVersionString(firstVersion);
    int[] secondVersionComponents = parseVersionString(secondVersion);
    // Compare the first 3 components (ie compare up to RC version).
    int comparison = compareNonRCVersions(firstVersionComponents, secondVersionComponents);
    if (comparison < 0) {
      // The first version is less than the second, and so is not newer.
      return false;
    } else if (comparison > 0) {
      // The first version is greater than the second, and so is newer.
      return true;
    } else {
      // Equality up to rc components.
      if (3 == firstVersionComponents.length && 3 == secondVersionComponents.length) {
        // Neither version has an RC component, so they're equal.
        return false;
      } else if (4 == firstVersionComponents.length && 3 == secondVersionComponents.length) {
        // First version is an RC, but the second isn't, so the second is newer.
        return false;
      } else if (3 == firstVersionComponents.length && 4 == secondVersionComponents.length) {
        // Second version is an RC, but the first isn't, so the first is newer.
        return true;
      } else {
        // Both versions have an RC component, so let's compare them.
        return firstVersionComponents[3] > secondVersionComponents[3];
      }
    }
  }

  /**
   * Determines if the latest version included with this upgrade response is later than the
   * specified version.
   *
   * @param version to compare against.
   * @return <code>true</code> if the latest version of this upgrade response is later than the
   *     specified version, <code>false</code> otherwise.
   */
  public boolean isLatestNewer(String version) {
    return null == mLatestVersion ? false : isVersionNewer(mLatestVersion, version);
  }

  /**
   * Determines if the compatible version included with this upgrade response is later than the
   * specified version.
   *
   * @param version to compare against.
   * @return <code>true</code> if the compatible version of this upgrade response is later than the
   *     specified version, <code>false</code> otherwise.
   */
  public boolean isCompatibleNewer(String version) {
    return null == mCompatibleVersion ? false : isVersionNewer(mCompatibleVersion, version);
  }

  /**
   * Determines if this upgrade response contains information on a relevant upgrade for the
   * specified version.
   *
   * @param currentVersion the version to check against.
   * @return <code>true</code> if either the compatible version or latest version in this upgrade
   *     response is newer than the specified version, <code>false</code> otherwise.
   */
  public boolean isRelevant(String currentVersion) {
    return isCompatibleNewer(currentVersion) || isLatestNewer(currentVersion);
  }

  /**
   * Gets a message that can be displayed to the user to remind them that an upgrade is available.
   * The message will include information on the latest version available and compatible version
   * available, depending on whether or not those upgrades are relevant to the current version
   * specified and if there is a difference between the two versions.
   *
   * @param currentVersion of the software to use when creating the upgrade message.
   * @return the message to display to the user reminding them of an upgrade, or the empty string
   *     if no versions in this upgrade info are relevant to the version specified.
   */
  public String getUpgradeReminder(String currentVersion) {
    // If both versions are missing, we're looking at a response with no upgrade.
    if (null == mLatestVersion && null == mCompatibleVersion) {
      return "";
    }

    if (null == mCompatibleVersion) {
      // There is only a latest version, and it is incompatible. Is it relevant?
      if (isLatestNewer(currentVersion)) {
        return String.format(INCOMPATIBLE_UPGRADE_MSG_FORMAT, mLatestVersion, currentVersion,
            mLatestVersionURL.toString(), mMessage);
      } else {
        // Nothing relevant in this upgrade info for the version specified.
        return "";
      }
    }

    if (mLatestVersion.equals(mCompatibleVersion)) {
      // We only care about the latest version, and it is compatible. Is it relevant?
      if (isLatestNewer(currentVersion)) {
        return String.format(COMPATIBLE_UPGRADE_MSG_FORMAT, mLatestVersion, currentVersion,
            mLatestVersionURL.toString(), mMessage);
      } else {
        // Nothing relevant in this upgrade info for the version specified.
        return "";
      }
    } else {
      // There are different compatible and incompatible versions available for upgrade.
      StringBuilder message = new StringBuilder("");
      if (isCompatibleNewer(currentVersion) && isLatestNewer(currentVersion)) {
        // Both versions are relevant upgrades.
        message.append(String.format(COMPATIBLE_UPGRADE_MSG_FORMAT, mCompatibleVersion,
            currentVersion, mCompatibleVersionURL.toString(), mMessage));
        message.append("\n");
        message.append(String.format(INCOMPATIBLE_UPGRADE_MSG_FORMAT, mLatestVersion,
            currentVersion, mLatestVersionURL.toString(), ""));
      } else if (isLatestNewer(currentVersion)) {
        // Only the latest version is a relevant upgrade and compatible version must be out of
        // date.
        message.append(String.format(INCOMPATIBLE_UPGRADE_MSG_FORMAT, mLatestVersion,
            currentVersion, mLatestVersionURL.toString(), ""));
      }
      // If we fall through without executing either of the above clauses, neither upgrade is
      // relevant and we're returning the empty string.
      return message.toString();
    }
  }

  /**
   * @return the format of this upgrade response.
   */
  public String getResponseFormat() {
    return mFormat;
  }

  /**
   * @return the latest version available for upgrade.
   */
  public String getLatestVersion() {
    return mLatestVersion;
  }

  /**
   * @return the URL where the latest version available for upgrade can be downloaded.
   */
  public URL getLatestVersionURL() {
    return mLatestVersionURL;
  }

  /**
   * @return the latest compatible version available for upgrade.
   */
  public String getCompatibleVersion() {
    return mCompatibleVersion;
  }

  /**
   * @return the URL where the latest compatible version available for upgrade can be downloaded.
   */
  public URL getCompatibleVersionURL() {
    return mCompatibleVersionURL;
  }

  /**
   * @return the message sent by the upgrade server about this upgrades.
   */
  public String getMessage() {
    return mMessage;
  }

  /**
   * @return the UNIX time the user was last reminded of this upgrade.
   */
  long getLastReminderTimeMillis() {
    return mLastReminderMillis;
  }
}
