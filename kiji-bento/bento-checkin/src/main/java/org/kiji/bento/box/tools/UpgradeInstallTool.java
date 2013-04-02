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

package org.kiji.bento.box.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.bento.box.UUIDTools;
import org.kiji.bento.box.UpgradeCheckin;
import org.kiji.bento.box.UpgradeResponse;
import org.kiji.bento.box.UpgradeServerClient;
import org.kiji.bento.box.VersionInfo;
import org.kiji.common.flags.Flag;
import org.kiji.common.flags.FlagParser;

/**
 * <p>A tool that performs an in-place upgrade to the next available Kiji BentoBox version.</p>
 *
 * <p>The tool will check for an upgrade with the updates server. If one is available, it will
 * download the next update, unzip it into a tmp dir, and run the installation bootstrap
 * process.</p>
 */
public final class UpgradeInstallTool {
  private static final Logger LOG = LoggerFactory.getLogger(UpgradeInstallTool.class);

  /** The path within a newly unzipped BentoBox to a bootstrap script to run. */
  private static final String BOOTSTRAP_SCRIPT_SUBPATH =
      "/cluster/bin/upgrade-kiji-bootstrap.sh";

  @Flag(name="bento-root", usage="The home directory of the BentoBox to upgrade in place.")
  private String mBentoRootPath = "";

  @Flag(name="tmp-dir", usage="Temporary directory where the upgrade can unpack files.")
  private String mBentoTmpDir = "/tmp";

  @Flag(name="upgrade-server-url",
      usage="The URL of an upgrade server to send check-in messages to.")
  private String mUpgradeServerURL = "";

  @Flag(name="dry-run", usage="Informs about new upgrades available, but doesn't run one.")
  private boolean mDryRun = false;

  /**
   * Checks the update server for an available update, and if one's available
   * downloads and installs it.
   *
   * @param args passed in from the command-line.
   * @return <code>0</code> if no errors are encountered, <code>1</code> otherwise.
   * @throws IOException if something goes wrong communicating with net or files.
   * @throws InterruptedException if we were interrupted while waiting for a subprocess.
   */
  public int run(String[] args) throws IOException, InterruptedException {
    // Parse the command-line arguments.
    final List<String> unparsed = FlagParser.init(this, args);
    if (null == unparsed) {
      LOG.error("There was an error parsing command-line flags. Cannot continue.");
      return 1;
    }

    if (mBentoRootPath.isEmpty()) {
      LOG.error("No bento root path specified by --bento-root");
      return 1;
    }

    if (mBentoTmpDir.isEmpty()) {
      LOG.error("No tmp dir specified by --tmp-dir. Try --tmp-dir=/tmp");
      return 1;
    }

    String currentVersion = VersionInfo.getSoftwareVersion();
    if (null == currentVersion) {
      LOG.error("Could not read current version info.");
      return 1;
    }

    // Create a client for the upgrade server, to get the latest version info.

    final URI checkinServerURI = UpgradeDaemonTool.getUpgradeServerURI(mUpgradeServerURL);
    final HttpClient httpClient = new DefaultHttpClient();
    final UpgradeServerClient upgradeClient =
        UpgradeServerClient.create(httpClient, checkinServerURI);

    UpgradeResponse response = null;
    try {
      // Get the saved upgrade response, or fail if there is none.
      final UpgradeCheckin request = new UpgradeCheckin.Builder()
            .withId(UUIDTools.getUserUUID())
            .withLastUsedMillis(System.currentTimeMillis())
            .build();
      response = upgradeClient.checkin(request);
      if (null == response) {
        System.out.println("Didn't receive a valid response from the upgrade server.");
        System.out.println("Please try again later, or check www.kiji.org for updates.");
        return 1;
      }
    } finally {
      httpClient.getConnectionManager().shutdown();
    }

    if (mDryRun) {
      return reportDryRunUpgrade(currentVersion, response);
    }

    int upgradeRet = runUpgrade(currentVersion, response);
    if (0 != upgradeRet) {
      System.out.println("Sorry, there was an error performing the upgrade process.");
    } else {
      System.out.println("Upgrade complete!");
    }
    return upgradeRet;
  }

  /**
   * Prints a message to a user running this in '--dry-run' mode indicating whether
   * an upgrade is available or not.
   *
   * <p>Returns an exit status for the application indicating upgrade available (success, 0)
   * or not available (failure, 1).
   *
   * @param currentVersion of the BentoBox.
   * @param upgradeInfo from the checkin server specifying where the new version is.
   * @return <code>0</code> if an upgrade is available, <code>1</code> otherwise.
   */
  private int reportDryRunUpgrade(String currentVersion, UpgradeResponse upgradeInfo) {
    if (!upgradeInfo.isCompatibleNewer(currentVersion)) {
      System.out.println("You are already running the latest BentoBox.");
      return 1; // No upgrade available.
    } else {
      System.out.println("A new update is available!");
      System.out.println("Current BentoBox version: " + currentVersion);
      System.out.println("Newest upgrade version: " + upgradeInfo.getCompatibleVersion());
      final URL upgradeUrl = upgradeInfo.getCompatibleVersionURL();
      System.out.println("To upgrade, type 'bento upgrade'. Or download it yourself at:");
      System.out.println(upgradeUrl.toString());
      return 0; // Upgrade is available.
    }
  }

  /**
   * Perform an upgrade to the compatible version specified by the upgrade response.
   *
   * @param currentVersion of the BentoBox.
   * @param upgradeInfo from the checkin server specifying where the new version is.
   * @return an exit status code of 0 for success, nonzero for error.
   * @throws IOException if there's a problem downloading the package.
   * @throws InterruptedException if there's an interrupt while waiting for a script to run.
   */
  private int runUpgrade(String currentVersion, UpgradeResponse upgradeInfo)
      throws IOException, InterruptedException {
    // If the upgrade information refers to a later version than the one currently running, and
    // if it's time to give a reminder, do so.
    if (!upgradeInfo.isCompatibleNewer(currentVersion)) {
      System.out.println("You are already running the latest BentoBox.");
      return 0; // Nothing to do.
    }

    System.out.println("A new update is available!");
    System.out.println("Current BentoBox version: " + currentVersion);
    System.out.println("Beginning upgrade to version: " + upgradeInfo.getCompatibleVersion());
    final File workingDir = makeWorkDir();
    final File targetFile = makeDownloadTarget(upgradeInfo, workingDir);
    final URL upgradeUrl = upgradeInfo.getCompatibleVersionURL();
    final HttpClient httpClient = new DefaultHttpClient();
    final HttpGet getReq = new HttpGet(upgradeUrl.toString());
    try {
      HttpResponse downloadResponse = httpClient.execute(getReq);

      int statusCode = downloadResponse.getStatusLine().getStatusCode();
      LOG.debug("Status code: " + statusCode);
      if (statusCode != HttpStatus.SC_OK) {
        System.out.println("Received error from HTTP server:\n");
        System.out.println(downloadResponse.getStatusLine().getReasonPhrase());
        return 1;
      }

      HttpEntity entity = downloadResponse.getEntity();
      if (null == entity) {
        System.out.println("Empty HTTP response when downloading from server.");
        System.out.println("Cannot automatically upgrade.");
        return 1;
      }

      OutputStream targetStream = new FileOutputStream(targetFile);
      try {
        entity.writeTo(targetStream);
      } finally {
        targetStream.close();
      }
    } finally {
      getReq.releaseConnection();
      httpClient.getConnectionManager().shutdown();
    }

    // Run tar vzxf on the tarball.
    final String unzippedDirName = getUnzippedDir(targetFile);
    unzip(workingDir, targetFile);

    LOG.info("Running bootstrap command in new instance...");

    // Run the upgrade bootstrap script from this tarball.
    final String[] command = {
        makeBootstrapCmd(workingDir, unzippedDirName).toString(),
        mBentoRootPath,
        currentVersion,
    };
    debugLogStringArray("Bootstrap command:", command);
    final ProcessBuilder bootstrapProcBuilder = new ProcessBuilder(command)
        .redirectErrorStream(true)
        .directory(workingDir);

    // Configure the environment for this process to run in.
    // We want to do so in as minimal an environment as possible.
    final Map<String, String> env = bootstrapProcBuilder.environment();
    final String javaHome = env.get("JAVA_HOME");
    env.clear(); // Run script in pure/empty environment.
    env.put("USER", System.getProperty("user.name"));
    env.put("HOME", System.getProperty("user.home"));
    env.put("JAVA_HOME", javaHome);

    final Process bootstrapProcess = bootstrapProcBuilder.start();
    printProcessOutput(bootstrapProcess);
    return bootstrapProcess.waitFor(); // Return its exit code.
  }

  /**
   * Take all stdout/stderr from a running subprocess and print it to our stdout.
   *
   * @param proc the process to consume input from.
   * @throws IOException if there's an error reading from the stream.
   */
  private static void printProcessOutput(Process proc) throws IOException {
    final BufferedReader inputReader = new BufferedReader(new InputStreamReader(
        proc.getInputStream(), "UTF-8"));
    try {
      while (true) {
        // Read and echo all stdout/stderr from the subprocess here.
        String line = inputReader.readLine();
        if (null == line) {
          break;
        }

        System.out.println(line.trim());
      }
    } finally {
      IOUtils.closeQuietly(inputReader);
    }
  }

  /**
   * Print a set of values to the debug log.
   *
   * @param title the title of the set of values.
   * @param values the values to print.
   */
  private static void debugLogStringArray(String title, String[] values) {
   if (LOG.isDebugEnabled()) {
      LOG.debug(title);
      for (String val: values) {
        LOG.debug("  " + val);
      }
    }
  }

  /**
   * Untar the package file into the destination directory.
   *
   * @param destDir the target directory to receive files from the package.
   * @param packageFile the .tar.gz file to unzip.
   * @throws IOException if there is an error performing the unzip operation.
   */
  private void unzip(File destDir, File packageFile) throws IOException {
    LOG.info("Unzipping " + packageFile + " into " + destDir);

    final String[] command = {
        "/usr/bin/env",
        "tar",
        "zxf",
        packageFile.getAbsolutePath(),
        "-C",
        destDir.getAbsolutePath(),
    };

    debugLogStringArray("Untar arguments:", command);

    final Process tarProc = new ProcessBuilder(command).start();
    try {
      printProcessOutput(tarProc);
      int exitStatus = tarProc.waitFor();
      if (0 != exitStatus) {
        throw new IOException("tar command did not exit correctly");
      }
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted while waiting for untar operation.");
    }
  }

  /**
   * Returns the name of the script that represents the bootstrap command to execute
   * inside the newly-unpacked BentoBox.
   *
   * @param workingDir the base directory where we unzip things to.
   * @param unzippedDirName the name of the subdirectory under workingDir where
   *     we bootstrap the new cluster.
   * @return the final path to the script to execute.
   */
  private File makeBootstrapCmd(File workingDir, String unzippedDirName) {
    return new File(workingDir, unzippedDirName + BOOTSTRAP_SCRIPT_SUBPATH);
  }

  /**
   * Create a directory that we can unzip things into.
   *
   * @return a File representing the directory where we can unzip files and do work.
   * @throws IOException if it can't make or delete a dir.
   */
  private File makeWorkDir() throws IOException {
    final File tmpDir = new File(mBentoTmpDir);
    if (!tmpDir.exists()) {
      // Try creating this path...
      if (!tmpDir.mkdirs()) {
        throw new RuntimeException("Temp dir could not be found or created: " + mBentoTmpDir);
      }
      assert tmpDir.exists();
    }

    // Try to create a brand new work dir.
    File workDir = null;
    Random rnd = new Random(System.currentTimeMillis());
    boolean createdDir = false;
    for (int attempts = 0; !createdDir && attempts < 25; attempts++) {
      workDir = new File(tmpDir, "bento-upgrade-tmp-" + System.getProperty("user.name")
          + "-" + rnd.nextInt());
      if (!workDir.exists()) {
        // It doesn't exist yet, try to create it.
        if (workDir.mkdir()) {
          assert workDir.exists();
          createdDir = true;
        }
      }
    }

    if (!createdDir) {
      throw new IOException("Couldn't make temp dir: " + workDir);
    }

    return workDir;
  }

  /**
   * Given the destination filename of the newly-downloaded bento cluster,
   * calculate the directory name it expands to.
   *
   * @param targetFile being downloaded. e.g. /path/to/kiji-bento-albacore-1.2.3-release.tar.gz
   * @return the unzipped release dir (kiji-bento-albacore)
   */
  private String getUnzippedDir(File targetFile) {
    final String basename = targetFile.getName();
    final String[] parts = basename.split("-");

    if (parts.length < 3) {
      LOG.error("Could not get unzipped dir: " + targetFile);
      return null;
    }

    if (!"kiji".equals(parts[0])) {
      LOG.error("Expected kiji-*:" + targetFile);
      return null;
    }
    if (!"bento".equals(parts[1])) {
      LOG.error("Expected kiji-bento-*:" + targetFile);
      return null;
    }

    final StringBuilder dir = new StringBuilder();
    dir.append(parts[0]);
    dir.append("-");
    dir.append(parts[1]);
    dir.append("-");
    dir.append(parts[2]);

    return dir.toString();
  }

  /**
   * Calculate the filename to save the download to.
   *
   * @param upgradeInfo the info about the Bento version available for upgrade.
   * @param workDir the directory where the download work is done.
   * @return a File representing the absolute path to write the download file to.
   */
  private File makeDownloadTarget(UpgradeResponse upgradeInfo, File workDir) {
    final URL upgradeUrl = upgradeInfo.getCompatibleVersionURL();
    // Get the /path/to/the/file.tar.gz from the URL.
    final File urlFile = new File(upgradeUrl.getFile());
    return new File(workDir, urlFile.getName());
  }


  /**
   * Java program entry point.
   *
   * @param args passed in from the command-line.
   * @throws Exception if something in this tool throws an exception.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new UpgradeInstallTool().run(args));
  }
}
