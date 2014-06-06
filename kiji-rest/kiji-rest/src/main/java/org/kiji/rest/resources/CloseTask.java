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

package org.kiji.rest.resources;

import java.io.PrintWriter;
import java.util.Collection;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMultimap;
import com.yammer.dropwizard.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.rest.KijiClient;

/**
 * This REST task allows administrators to close open Kiji instances and tables so that
 * they can be deleted.  Note that closing an instance or table is not permanent.  If the closed
 * instance or table is subsequently required to fulfill a request, it will be recreated.
 * Therefore, a table or instance with active users should not be closed. Once all users have
 * stopped hitting the instance or table, it can be closed in this REST instance and dropped.
 */
public class CloseTask extends Task {
  private static final Logger LOG = LoggerFactory.getLogger(CloseTask.class);
  public static final String INSTANCE_KEY = "instance";
  public static final String TABLE_KEY = "table";

  private final KijiClient mKijiClient;

  /**
   * Create a CloseTask with the provided KijiClient.
   *
   * @param kijiClient to use to close instances and tables.
   */
  public CloseTask(KijiClient kijiClient) {
    super("close");
    mKijiClient = kijiClient;
  }

  /** {@inheritDoc} */
  @Override
  public void execute(
      ImmutableMultimap<String, String> parameters,
      PrintWriter output
  ) throws Exception {
    final Collection<String> instances = Preconditions.checkNotNull(parameters.get(INSTANCE_KEY));
    Preconditions.checkArgument(instances.size() == 1, "Must supply a single instance.");
    final String instance = instances.iterator().next();

    final Collection<String> tables = parameters.get(TABLE_KEY);
    if (tables.isEmpty()) {
      // Close the instance
      mKijiClient.invalidateInstance(instance);
    } else {
      Preconditions.checkArgument(tables.size() == 1, "Can not close multiple tables: %s.", tables);
      final String table = tables.iterator().next();
      mKijiClient.invalidateTable(instance, table);
    }
  }
}
