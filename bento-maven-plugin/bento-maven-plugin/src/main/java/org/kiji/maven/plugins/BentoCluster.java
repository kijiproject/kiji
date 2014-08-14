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

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.logging.Log;

/**
 * An in-process way to start and stop a Bento cluster running in a Docker container. This class
 * wraps commands sent to the bento script in the Bento cluster installation.
 */
public enum BentoCluster {
  /** The singleton instance. */
  INSTANCE;

  /**
   * Format for running bento commands: {path/to/bento-cluster/bin/bento} {command} -n {bento-name}.
   */
  private static final String BENTO_SHELL_SCRIPT_FORMAT = "%s %s -n %s";
  /**
   * Format for querying supervisorctl to check whether the Bento cluster components are up. The
   * the only required configurable component is the bento container name.
   */
  private static final String SUPERVISORCTL_HDFS_INIT_STATUS =
      "supervisorctl -c %s -s http://bento-%s:9001 status hdfs-init";

  /**
   * Bento commands.
   */
  private static final String BENTO_CREATE = "create";
  private static final String BENTO_STOP = "stop";
  private static final String BENTO_RM = "rm";
  private static final String BENTO_STATUS = "status";

  /** Hope the the bento cluster components all start in 120 seconds. */
  private static final long STARTUP_TIMEOUT_MS = 120000L;

  /** Where on this machine the bento-cluster installation lives. */
  private File mBentoDirPath;

  /** Name of the Bento cluster container. */
  private String mBentoName;

  /** The maven log used to communicate with the maven user. */
  private Log mLog;

  /**
   * Returns a bento script shell command to execute.
   *
   * @param command to run in shell such as create, stop, rm, etc.
   * @return a formatted string command to execute on the shell.
   */
  private String bentoCommand(String command) {
    return String.format(
        BENTO_SHELL_SCRIPT_FORMAT,
        new File(new File(mBentoDirPath, "bin"), "bento").toString(),
        command,
        mBentoName
    );
  }

  /**
   * Disable default constructor.
   */
  private BentoCluster() {}

  /**
   * Execute command to start the Bento cluster container within a timeout.
   *
   * @param log The maven log.
   * @param bentoDirPath path to the bento-cluster environment installation.
   * @param bentoName name of the Bento cluster container.
   * @throws Exception if the Bento cluster container could not be started in the specified timeout.
   */
  public void start(Log log, File bentoDirPath, String bentoName) throws Exception {
    mLog = log;
    mBentoDirPath = bentoDirPath;
    mBentoName = bentoName;

    if (isRunning()) {
      throw new RuntimeException("Cluster already running.");
    }

    // Start Bento cluster by running the Bento create script.
    mLog.info(String.format("Starting the Bento cluster '%s'...", mBentoName));
    mLog.info(ShellExecUtil.executeCommand(bentoCommand(BENTO_CREATE)));

    // Has the container started as expected within the startup timeout?
    final long startTime = System.currentTimeMillis();
    while (!isRunning()) {
      if (System.currentTimeMillis() - startTime > STARTUP_TIMEOUT_MS) {
        throw new RuntimeException(String.format(
            "Could not start the Bento cluster '%s' within required timeout %d.",
            mBentoName,
            STARTUP_TIMEOUT_MS));
      }
      Thread.sleep(1000);
    }
  }

  /**
   * Execute command to stop the Bento cluster container. Wait uninterruptibly until the shell
   * command returns.
   *
   * @throws Exception if the Bento cluster container could not be stopped.
   */
  public void stop() throws Exception {
    if (!isRunning()) {
      mLog.error(
          "Attempting to shut down a Bento cluster container, but none running.");
      return;
    }

    mLog.info(String.format("Stopping the Bento cluster '%s'...", mBentoName));
    mLog.info(ShellExecUtil.executeCommand(bentoCommand(BENTO_STOP)));
    mLog.info(ShellExecUtil.executeCommand(bentoCommand(BENTO_RM)));
  }

  /**
   * Check if the Bento cluster container is running, by querying the bento script.
   *
   * @return true if the bento script
   * @throws Exception if the bento script can not be uninterruptibly queried for whether a bento
   * container is running.
   */
  public boolean isRunning() throws Exception {
    return ShellExecUtil.executeCommand(bentoCommand(BENTO_STATUS)).contains("started")
        && isRunningPerSupervisorCtl();
  }

  /**
   * Query supervisorctl to see whether the Bento cluster components (HDFS,
   * etc.) have started. We do this by checking whether hdfs-init has exited.
   *
   * This functionality requires that supervisor's RPC interface is set up.
   *
   * @return true if hdfs-init has exited, meaning that our components are up.
   * @throws Exception if the supervisorctl query fails.
   */
  private boolean isRunningPerSupervisorCtl() throws Exception {
    // In order to operate supervisorctl, we are required to specify a local config file even if
    // it is unused. So we create a bare config file.
    File bareSupervisorConfig;
    try {
      bareSupervisorConfig = File.createTempFile("empty_supervisor_config", ".conf");
      FileUtils.write(bareSupervisorConfig, "[supervisorctl]");
    } catch (IOException ioe) {
      throw new IOException(
          "Unable to write bare config to operate supervisorctl to query bento container status.",
          ioe);
    }
    // Check whether hdfs-init has exited, meaning that the required components are up.
    return ShellExecUtil.executeCommand(String.format(SUPERVISORCTL_HDFS_INIT_STATUS,
        bareSupervisorConfig.getAbsolutePath(),
        mBentoName)).contains("EXITED");
  }
}
