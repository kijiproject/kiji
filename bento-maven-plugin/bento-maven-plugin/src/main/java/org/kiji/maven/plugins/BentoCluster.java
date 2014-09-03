/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.maven.plugins;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;

/**
 * An in-process way to start and stop a Bento cluster running in a Docker container. This class
 * wraps commands sent to the bento script in the Bento cluster installation.
 */
public final class BentoCluster {
  /**
   * Where in the bento-cluster environment are the generated site files stored.
   */
  private static final String[] SITE_FILE_RELATIVE_PATHS = new String[] {
      "hbase/hbase-site.xml",
      "hadoop/core-site.xml",
      "hadoop/mapred-site.xml",
      "hadoop/yarn-site.xml",
  };

  /** The singleton instance. */
  private static BentoCluster instance = null;

  /**
   * Initializes the singleton.
   *
   * @param bentoName name of the Bento cluster container.
   * @param venvRoot to the python virtualenv to install bento-cluster to.
   * @param dockerAddress of the docker daemon to use to manage bento instances.
   * @param mavenLog the maven log.
   */
  public static void setInstance(
      final String bentoName,
      final File venvRoot,
      final String dockerAddress,
      final Log mavenLog
  ) {
    Preconditions.checkState(
        venvRoot.mkdirs() || venvRoot.exists(),
        "Failed to create venv root: %s",
        venvRoot.getAbsolutePath()
    );
    instance = new BentoCluster(bentoName, venvRoot, dockerAddress, mavenLog);
  }

  /**
   * Returns true if the singleton has been initialized.
   *
   * @return true if the singleton has been initialized.
   */
  public static boolean isInstanceSet() {
    return instance != null;
  }

  /**
   * Gets the singleton instance.
   *
   * @return the singleton instance.
   */
  public static BentoCluster getInstance() {
    return instance;
  }

  /**
   * Returns the address to connect to docker with.
   *
   * If the OS is linux, returns a connection string for the docker instance on the default local
   * unix socket. If the OS is mac, returns a connection string for the boot2docker instance stored
   * in the environment variable DOCKER_HOST.
   *
   * @return the address to connect to docker with.
   */
  public static String getDockerAddress() {
    final String osNameProperty = System.getProperty("os.name");
    if (osNameProperty.contains("Linux")) {
      return "unix://";
    } else if (osNameProperty.contains("OS X")) {
      Preconditions.checkState(
          !System.getenv().containsKey("DOCKER_HOST"),
          "Please set environment variable DOCKER_HOST to contain the address to boot2docker"
      );
      return System.getenv("DOCKER_HOST");
    } else {
      throw new IllegalStateException(
          String.format("Unsupported operating system: %s", osNameProperty)
      );
    }
  }

  // - Non-static ----------------------------------------------------------------------------------

  /** Name of the Bento cluster container. */
  private final String mBentoName;

  /** Path to the python virtualenv to install bento-cluster to. */
  private final File mVenvRoot;

  /** Address of the docker daemon to use to manage bento instances. */
  private final String mDockerAddress;

  /** The maven log used to communicate with the maven user. */
  private final Log mMavenLog;

  /**
   * Constructs a bento cluster.
   *
   * @param bentoName name of the Bento cluster container.
   * @param venvRoot to the python virtualenv to install bento-cluster to.
   * @param dockerAddress of the docker daemon to use to manage bento instances.
   * @param mavenLog to use to send output to the maven console.
   */
  private BentoCluster(
      final String bentoName,
      final File venvRoot,
      final String dockerAddress,
      final Log mavenLog
  ) {
    mBentoName = bentoName;
    mVenvRoot = venvRoot;
    mDockerAddress = dockerAddress;
    mMavenLog = mavenLog;
  }

  /**
   * Execute command to start the Bento cluster container within a timeout.
   *
   * @param configDir to write bento hadoop/hbase configuration files to.
   * @param createBento should be set to false to skip creating the bento container.
   * @param platformVersion is the optional version of the hadoop/hbase stack to run in the bento
   *     cluster.
   * @param pypiRepository is the optional address or name of the pypi repository to install
   *     kiji-bento-cluster from.
   * @param timeoutMillis is the optional override for the amount of time to wait in milliseconds
   *     for the bento cluster to start.
   * @param pollIntervalMillis is the optional override for the amount of time to wait in
   *     milliseconds between checks to see if the bento cluster has started.
   * @throws java.io.IOException if there is an error calling the bento executable.
   * @throws java.util.concurrent.ExecutionException if there is an error calling the bento
   *     executable.
   */
  public void start(
      final File configDir,
      final boolean createBento,
      final Optional<String> platformVersion,
      final Optional<String> pypiRepository,
      final Optional<Long> timeoutMillis,
      final Optional<Long> pollIntervalMillis
  ) throws IOException, ExecutionException {
    Preconditions.checkState(!isRunning(), "Cluster already running: %s", this);

    mMavenLog.info(String.format("Starting the Bento cluster '%s'...", mBentoName));

    final ImmutableList.Builder<String> commandsBuilder = ImmutableList.builder();

    // If one doesn't exist already create a virtualenv for this cluster.
    if (!venvExists()) {
      commandsBuilder.add(String.format("env python3 -m venv %s", mVenvRoot.getAbsolutePath()));
    }

    // Setup the shell environment for the python virtual environment.
    commandsBuilder.add(sourceVenvCommand());

    // Install bento-cluster in this virtualenv.
    if (pypiRepository.isPresent()) {
      commandsBuilder.add(
          String.format("env python3 -m pip install kiji-bento-cluster -i %s", pypiRepository.get())
      );
    } else {
      commandsBuilder.add("env python3 -m pip install kiji-bento-cluster");
    }

    // Add a command to create the bento container.
    if (createBento) {
      final StringBuilder pullCommandBuilder = new StringBuilder("pull");
      if (platformVersion.isPresent()) {
        pullCommandBuilder.append(" --platform-version=").append(platformVersion.get());
      }
      commandsBuilder.add(bentoCommand(pullCommandBuilder.toString()));

      final StringBuilder createCommandBuilder = new StringBuilder("create")
          .append(" --output-config-dir=").append(configDir.getAbsolutePath());
      if (platformVersion.isPresent()) {
        createCommandBuilder.append(" --platform-version=").append(platformVersion.get());
      }
      commandsBuilder.add(bentoCommand(createCommandBuilder.toString()));
    }

    // Add a command to start the bento container.
    final StringBuilder startCommandBuilder = new StringBuilder("start")
        .append(" --output-config-dir=").append(configDir.getAbsolutePath());
    if (timeoutMillis.isPresent()) {
      startCommandBuilder.append(" --timeout=").append(timeoutMillis.get());
    }
    if (pollIntervalMillis.isPresent()) {
      startCommandBuilder.append(" --poll-interval=").append(pollIntervalMillis.get());
    }
    commandsBuilder.add(bentoCommand(startCommandBuilder.toString()));

    // Run the commands.
    final ImmutableList<String> commands = commandsBuilder.build();
    executeAndLogCommands(commands.toArray(new String[commands.size()]));

    // Ensure that the cluster has been started.
    Preconditions.checkState(isRunning(), "Bento cluster failed to start!");
  }

  /**
   * Execute command to stop the Bento cluster container. Wait uninterruptibly until the shell
   * command returns.
   *
   * @param deleteBento should be set to false to skip deleting the bento container.
   * @param timeoutMillis is the optional override for the amount of time to wait in milliseconds
   *     for the bento cluster to stop.
   * @param pollIntervalMillis is the optional override for the amount of time to wait in
   *     milliseconds between checks to see if the bento cluster has stopped.
   * @throws java.io.IOException if there is an error calling the bento executable.
   * @throws java.util.concurrent.ExecutionException if there is an error calling the bento
   *     executable.
   */
  public void stop(
      final boolean deleteBento,
      final Optional<Long> timeoutMillis,
      final Optional<Long> pollIntervalMillis
  ) throws IOException, ExecutionException {
    if (!isRunning()) {
      mMavenLog.error("Attempting to shut down a Bento cluster container, but none running.");
      return;
    }

    final ImmutableList.Builder<String> commandsBuilder = ImmutableList.builder();

    // Add a command to activate the python virtual environment for this plugin.
    commandsBuilder.add(sourceVenvCommand());

    // Add a command to stop the bento cluster.
    final StringBuilder stopCommandBuilder = new StringBuilder("stop");
    if (timeoutMillis.isPresent()) {
      stopCommandBuilder.append(" --timeout=").append(timeoutMillis.get());
    }
    if (pollIntervalMillis.isPresent()) {
      stopCommandBuilder.append(" --poll-interval").append(pollIntervalMillis.get());
    }
    commandsBuilder.add(bentoCommand(stopCommandBuilder.toString()));

    // Add a command to delete the bento cluster.
    if (deleteBento) {
      commandsBuilder.add(bentoCommand("rm"));
    }

    // Run the commands.
    final ImmutableList<String> commands = commandsBuilder.build();
    mMavenLog.info(String.format("Stopping the Bento cluster '%s'...", mBentoName));
    executeAndLogCommands(commands.toArray(new String[commands.size()]));
  }

  /**
   * Check if the Bento cluster services are running, by querying the bento script.
   *
   * @return true if the bento cluster init script has completed.
   * @throws java.io.IOException if the bento script can not be uninterruptibly queried for whether
   *     a bento container is running.
   * @throws java.util.concurrent.ExecutionException if the bento script can not be uninterruptibly
   *     queried for whether a bento container is running.
   */
  public boolean isRunning() throws IOException, ExecutionException {
    return venvExists() && executeAndLogCommands(sourceVenvCommand(), bentoCommand("status"))
        .getStderr()
        .contains("services started");
  }

  /**
   * Check if the Bento cluster container is running, by querying the bento script.
   *
   * @return true if the bento cluster docker container is running.
   * @throws java.io.IOException if the bento script can not be uninterruptibly queried for whether
   *     a bento container is running.
   * @throws java.util.concurrent.ExecutionException if the bento script can not be uninterruptibly
   *     queried for whether a bento container is running.
   */
  public boolean isContainerRunning() throws IOException, ExecutionException {
    return venvExists() && executeAndLogCommands(sourceVenvCommand(), bentoCommand("status"))
        .getStderr()
        .contains("container started");
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("bentoName", mBentoName)
        .add("venvRoot", mVenvRoot)
        .add("dockerAddress", mDockerAddress)
        .toString();
  }

  /**
   * Returns true if this bento cluster's venv exists.
   *
   * @return true if this bento cluster's venv exists.
   */
  private boolean venvExists() {
    return new File(new File(mVenvRoot, "bin"), "activate").exists();
  }

  /**
   * Returns a command that sources a python venv's activate script.
   *
   * @return a command that sources a python venv's activate script.
   */
  private String sourceVenvCommand() {
    return String.format("source %s/bin/activate", mVenvRoot.getAbsolutePath());
  }

  /**
   * Returns a bento script shell command to execute.
   *
   * @param command to run in shell such as create, stop, rm, etc.
   * @return a formatted string command to execute.
   */
  private String bentoCommand(final String command) {
    return String.format(
        "bento --docker-address=%s --bento-name=%s %s",
        mDockerAddress,
        mBentoName,
        command
    );
  }

  /**
   * Copy a site file generated by the bento script to the location specified by the plugin
   * specification.
   *
   * @param bentoConfigDir to copy site files from.
   * @param pluginConfigDir to copy site files to.
   * @throws MojoExecutionException if copying the site file or writing to the index file fails.
   */
  public void copySiteFiles(
      final File bentoConfigDir,
      final File pluginConfigDir
  ) throws MojoExecutionException {
    for (final String siteFilePath : SITE_FILE_RELATIVE_PATHS) {
      copySiteFile(new File(bentoConfigDir, siteFilePath), pluginConfigDir);
    }
  }

  /**
   * Copy a site file generated by the bento script to the location specified by the plugin
   * specification.
   *
   * @param generatedSiteFile is the path to the site file generated by bento to copy.
   * @param pluginConfigDir to copy the site file to.
   * @throws MojoExecutionException if copying the site file or writing to the index file fails.
   */
  private void copySiteFile(
      final File generatedSiteFile,
      final File pluginConfigDir
  ) throws MojoExecutionException {
    try {
      FileUtils.copyFileToDirectory(generatedSiteFile, pluginConfigDir);
    } catch (final IOException ioe) {
      throw new MojoExecutionException(
          String.format(
              "Copying site file %s to location %s failed.",
              generatedSiteFile.getAbsolutePath(),
              pluginConfigDir.getAbsolutePath()
          ),
          ioe
      );
    }
    final File writtenFile = new File(pluginConfigDir, generatedSiteFile.getName());
    Preconditions.checkArgument(writtenFile.exists());
    mMavenLog.info("Wrote config site file: " + writtenFile.getAbsolutePath());

    // We will also append the "conf-index.conf" file with the path to the newly written config
    // site file.
    final File confIndexFile = new File(pluginConfigDir, "conf-index.conf");
    try {
      FileUtils.write(confIndexFile, writtenFile.getAbsolutePath() + "\n", true);
    } catch (final IOException ioe) {
      throw new MojoExecutionException(
          String.format(
              "Unable to write to configuration index file: %s",
              writtenFile.getAbsolutePath()
          ),
          ioe
      );
    }
    mMavenLog.info("Appended site file path to conf index: " + confIndexFile.getAbsolutePath());
  }

  /**
   * Executes a list of commands and logs its stdout/stderr and exit code results to the maven log.
   *
   * @param commands to run.
   * @return the script file that commands were written to.
   * @throws java.io.IOException if there is an error running the command.
   * @throws java.util.concurrent.ExecutionException if there is an error running the command.
   */
  private ShellResult executeAndLogCommands(
      final String... commands
  ) throws IOException, ExecutionException {
    final File scriptFile = File.createTempFile(
        String.format("%s-command-", mBentoName),
        ".sh",
        mVenvRoot
    );
    return executeAndLogCommands(scriptFile, commands);
  }

  /**
   * Executes a list of commands and logs its stdout/stderr and exit code results to the maven log.
   *
   * @param scriptFile to write commands to.
   * @param commands to run.
   * @return the script file that commands were written to.
   * @throws java.io.IOException if there is an error running the command.
   * @throws java.util.concurrent.ExecutionException if there is an error running the command.
   */
  private ShellResult executeAndLogCommands(
      final File scriptFile,
      final String... commands
  ) throws IOException, ExecutionException {
    if (!scriptFile.exists()) {
      Preconditions.checkState(
          scriptFile.createNewFile(),
          "Failed to create file: %s",
          scriptFile
      );
    }

    // Write commands to a bash shell script.
    final PrintWriter writer = new PrintWriter(scriptFile, "utf-8");
    try {
      writer.println("#!/bin/bash");
      writer.println();
      writer.println("set -e");
      writer.println("set -x");
      writer.println();
      writer.println("# User commands:");
      for (final String command : commands) {
        writer.println(command);
      }
    } finally {
      writer.flush();
      writer.close();
    }

    // Run the script.
    final ShellResult result = ShellExecUtil.executeCommand(
        String.format("/bin/bash %s", scriptFile.getAbsolutePath())
    );

    // Log stdout/stderr.
    mMavenLog.info("Stdout:");
    mMavenLog.info(result.getStdout());
    mMavenLog.info("Stderr:");
    mMavenLog.info(result.getStderr());
    mMavenLog.info(String.format("Exit code: %d", result.getExitCode()));

    Preconditions.checkState(
        result.getExitCode() == 0,
        "Expected result of command to be 0"
    );

    return result;
  }
}
