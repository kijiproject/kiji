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
public class StopMojo extends AbstractMojo {
  /** If true, this goal should be a no-op. */
  @Parameter(property = "skip", defaultValue = "false")
  private boolean mSkip;

  /** If persist is true, then the started Bento cluster is persisted. */
  @Parameter(property = "persist", defaultValue = "false")
  private boolean mPersist;

  /** {@inheritDoc} */
  @Override
  public void execute() throws MojoExecutionException {
    if (mSkip) {
      getLog().info("Not stopping an Bento cluster because skip=true.");
      return;
    }

    if (mPersist) {
      getLog().info("Not stopping an Bento cluster because persist=true.");
      return;
    }

    // Start the cluster.
    try {
      BentoCluster.INSTANCE.stop();
    } catch (Exception e) {
      throw new MojoExecutionException("Unable to stop Bento cluster.", e);
    }
  }
}
