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

package org.kiji.rest.resources;

import java.io.PrintWriter;

import com.google.common.collect.ImmutableMultimap;
import com.yammer.dropwizard.tasks.Task;

import org.kiji.annotations.ApiAudience;
import org.kiji.rest.KijiRESTService.RefreshInstances;

/**
 * This REST resource interacts with Kiji instances collection resource.
 *
 * This resource is served for requests using the resource identifier:
 * <li>/v1/instances/
 */
@ApiAudience.Public
public class RefreshInstancesTask extends Task {
  private final Runnable mInstanceRefresher;

  /**
   * Default constructor.
   *
   * @param instanceRefresher is the runnable object which refreshes instances on a cluster.
   */
  public RefreshInstancesTask(final RefreshInstances instanceRefresher) {
    super("refresh_instances");
    mInstanceRefresher = instanceRefresher;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> arg0, PrintWriter arg1)
      throws Exception {
    mInstanceRefresher.run();
    arg1.println("Manually refreshed instances...");
    arg1.flush();
  }
}
