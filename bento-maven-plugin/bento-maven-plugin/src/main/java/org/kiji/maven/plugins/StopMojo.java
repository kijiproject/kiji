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
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * A maven goal that stops the Bento cluster started by the 'start' goal.
 */
@Mojo(
    name = "stop",
    defaultPhase = LifecyclePhase.POST_INTEGRATION_TEST
)
public final class StopMojo extends AbstractMojo {
  /**
   * Address of the docker daemon to manage bento instances with.
   */
  @Parameter(
      property = "bento.docker.address",
      alias = "bento.docker.address",
      required = false
  )
  private String mDockerAddress;

  /**
   * If true, this goal should be a no-op.
   */
  @Parameter(
      property = "bento.skip",
      alias = "bento.skip",
      defaultValue = "false",
      required = false
  )
  private boolean mSkip;

  /**
   * Optional bento instance name override. Can be used to use an existing bento instance.
   */
  @Parameter(
      property = "bento.name",
      alias = "bento.name",
      required = false
  )
  private String mBentoName;

  /**
   * Python venv root to install the bento cluster to.
   */
  @Parameter(
      property = "bento.venv",
      alias = "bento.venv",
      defaultValue = "${project.build.directory}/bento-maven-plugin-venv",
      required = false
  )
  private File mBentoVenvRoot;

  /**
   * If true, skips stopping the bento instance.
   */
  @Parameter(
      property = "bento.skip.stop",
      alias = "bento.skip.stop",
      defaultValue = "false",
      required = false
  )
  private boolean mSkipBentoStop;

  /**
   * If true, skips deleting the bento instance.
   */
  @Parameter(
      property = "bento.skip.rm",
      alias = "bento.skip.rm",
      defaultValue = "false",
      required = false
  )
  private boolean mSkipBentoRm;

  /**
   * Time to wait for the bento cluster to start.
   */
  @Parameter(
      property = "bento.timeout.start",
      alias = "bento.timeout.start",
      required = false
  )
  // This is a Long so that it will be set to null if this flag is not provided.
  private Long mTimeout;

  /**
   * Interval at which to poll the bento cluster's status when starting or stopping it.
   */
  @Parameter(
      property = "bento.timeout.interval",
      alias = "bento.timeout.interval",
      required = false
  )
  // This is a Long so that it will be set to null if this flag is not provided.
  private Long mPollInterval;

  /**
   * Default constructor for reflection.
   */
  public StopMojo() { }

  // We're going to use a bunch of parameters here just for the purpose of a test.
  // CSOFF: ParameterNumber
  /**
   * Constructor for tests.
   *
   * @param dockerAddress flag.
   * @param skip flag.
   * @param bentoName flag.
   * @param bentoVenvRoot flag.
   * @param skipBentoStop flag.
   * @param skipBentoRm flag.
   * @param timeout flag.
   * @param pollInterval flag.
   */
  public StopMojo(
      final String dockerAddress, final boolean skip,
      final String bentoName,
      final File bentoVenvRoot,
      final boolean skipBentoStop,
      final boolean skipBentoRm,
      final Long timeout,
      final Long pollInterval
  ) {
    mDockerAddress = dockerAddress;
    mSkip = skip;
    mBentoName = bentoName;
    mBentoVenvRoot = bentoVenvRoot;
    mSkipBentoStop = skipBentoStop;
    mSkipBentoRm = skipBentoRm;
    mTimeout = timeout;
    mPollInterval = pollInterval;
  }
  // CSON: ParameterNumber

  /** {@inheritDoc} */
  @Override
  public void execute() throws MojoExecutionException {
    if (mSkip) {
      getLog().info("Not stopping an Bento cluster because bento.skip=true.");
      return;
    }

    if (mSkipBentoStop) {
      getLog().info("Not stopping an Bento cluster because bento.skip.stop=true.");
      return;
    }

    // Stop the cluster.
    if (!BentoCluster.isInstanceSet()) {
      Preconditions.checkArgument(
          null != mBentoName,
          "A bento name must be provided if a bento wasn't started by this plugin."
      );
      BentoCluster.setInstance(
          mBentoName,
          mBentoVenvRoot,
          mDockerAddress == null ? BentoCluster.getDockerAddress() : mDockerAddress,
          getLog()
      );
    }
    try {
      BentoCluster.getInstance().stop(
          !mSkipBentoRm,
          Optional.fromNullable(mTimeout),
          Optional.fromNullable(mPollInterval)
      );
    } catch (final IOException ioe) {
      throw new MojoExecutionException("Failed to stop bento cluster", ioe);
    } catch (final ExecutionException ee) {
      throw new MojoExecutionException("Failed to stop bento cluster", ee);
    }
  }
}
