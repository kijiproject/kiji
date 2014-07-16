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

package org.kiji.mapreduce.framework;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Combines subsplits into InputSplits.
 *
 * This class attempts to combine subplits such that:
 * <ul>
 *   <li>The final set of InputSplits matches the user's requested number of InputSplits</li>
 *   <li>As many subsplits as possible share a common replica node</li>
 * </ul>
 */
class CassandraSubSplitCombiner {

  /**
   * Combine subsplits into InputSplits, attempting to group together subsplits that share replica
   * nodes.
   * @param subsplits A collection of subsplits to combine.
   * @param targetNumSplits Target number of input splits to have after combining subsplits.
   * @return A list of InputSplits.
   */
  public List<CassandraInputSplit> combineSubsplits(
      Collection<CassandraSubSplit> subsplits, int targetNumSplits) {

    // Estimate the number of subsplits per input split.
    final int numSubsplits = subsplits.size();
    final int numSubsplitsPerSplit =
        numSubsplits / targetNumSplits;

    // Group subsplits by host and try to combine subsplits that share a host.
    List<CassandraSubSplit> subsplitsSortedByHost = getSubsplitsSortedByHost(subsplits);

    List<CassandraInputSplit> inputSplits = Lists.newArrayList();

    int subsplitIndex = 0;
    while (subsplitIndex < numSubsplits) {

      // Start a new InputSplit.
      Set<CassandraSubSplit> subsplitsToCombine = Sets.newHashSet();

      // Go until we get to our target number of subsplits / input split.
      while (true) {
        // No more data => can't add to this InputSplit anymore.
        if (subsplitIndex >= numSubsplits) {
          break;
        }

        // Add this subsplit to the current working input split.
        CassandraSubSplit subsplitToAdd = subsplitsSortedByHost.get(subsplitIndex);
        subsplitsToCombine.add(subsplitToAdd);
        subsplitIndex++;

        // If we have reached our size goal, then finish this input split.
        if (subsplitsToCombine.size() == numSubsplitsPerSplit) {
          break;
        }
      }

      assert(subsplitsToCombine.size() > 0);

      // Now create the input split.
      CassandraInputSplit inputSplit = CassandraInputSplit.createFromSubplits(subsplitsToCombine);
      inputSplits.add(inputSplit);
    }
    return inputSplits;
  }

  /**
   * Combine subsplits into InputSplits, attempting to group together subsplits that share replica
   * nodes.
   *
   * Will combine into four subsplits.
   *
   * @param subsplits A collection of subsplits to combine.
   * @return A list of InputSplits.
   */
  public List<CassandraInputSplit> combineSubsplits(Collection<CassandraSubSplit> subsplits) {
    return combineSubsplits(subsplits, 4);
  }

  /**
   * Sort subsplits by host.
   *
   * @param unsortedSubsplits An unsorted collection of subsplits.
   * @return A list of the subsplits, sorted by host.
   */
  private List<CassandraSubSplit> getSubsplitsSortedByHost(
      Collection<CassandraSubSplit> unsortedSubsplits) {
    List<CassandraSubSplit> subsplitsSortedByHost = Lists.newArrayList(unsortedSubsplits);
    Collections.sort(
        subsplitsSortedByHost,
        new Comparator<CassandraSubSplit>() {
          public int compare(CassandraSubSplit firstSubsplit, CassandraSubSplit secondSubsplit) {
            String firstHostList = firstSubsplit.getSortedHostListAsString();
            String secondHostList = secondSubsplit.getSortedHostListAsString();
            return firstHostList.compareTo(secondHostList);
          }
        }
    );
    return subsplitsSortedByHost;
  }
}
