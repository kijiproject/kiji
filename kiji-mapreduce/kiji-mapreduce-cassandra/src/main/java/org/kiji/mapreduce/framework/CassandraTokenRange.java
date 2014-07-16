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

/**
* Describes a Cassandra token range associated with a subsplit.
*/
final class CassandraTokenRange {
  /** Starting token (inclusive). */
  private final long mStartToken;

  /** Ending token (exclusive). */
  private final long mEndToken;

  /**
   * Construct a new token range.
   *
   * @param startToken for the token range (inclusive).
   * @param endToken for the token range (inclusive).
   */
  CassandraTokenRange(long startToken, long endToken) {
    this.mStartToken = startToken;
    this.mEndToken = endToken;
  }

  /**
   * @return the starting token for this token range.
   */
  long getStartToken() {
    return mStartToken;
  }

  /**
   * @return the ending token for this token range.
   */
  long getEndToken() {
    return mEndToken;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return String.format(
        "(%s, %s)",
        mStartToken,
        mEndToken
    );
  }
}
