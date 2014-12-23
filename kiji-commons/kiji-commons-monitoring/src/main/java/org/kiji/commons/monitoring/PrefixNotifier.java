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

package org.kiji.commons.monitoring;

import java.io.IOException;
import java.util.Map;

/**
 * A notifier which adds a constant prefix to the beginning of each action, and proxies the
 * notification to another notifier.
 */
public final class PrefixNotifier implements Notifier {
  private final String mPrefix;
  private final Notifier mNotifier;

  /**
   * Construct a new prefix notifier, which adds a constant prefix to all actions and passes each
   * notification to another notifier.
   *
   * @param prefix The prefix.
   * @param notifier The notifiers.
   */
  private PrefixNotifier(final String prefix, final Notifier notifier) {
    mPrefix = prefix;
    mNotifier = notifier;
  }

  /**
   * Create a new prefix notifier, which adds a constant prefix to all actions and passes each
   * notification to another notifier.
   *
   * @param prefix A prefix to add to each notification's action.
   * @param notifier The notifier to pass all notifications to.
   * @return A new prefix notifier.
   */
  public static PrefixNotifier create(final String prefix, final Notifier notifier) {
    // Avoid stacking multiple prefix notifiers.
    if (notifier instanceof PrefixNotifier) {
      final PrefixNotifier prefixNotifier = (PrefixNotifier) notifier;
      return new PrefixNotifier(prefixNotifier.mPrefix + "." + prefix, prefixNotifier.mNotifier);
    }

    return new PrefixNotifier(prefix + ".", notifier);
  }

  /** {@inheritDoc} */
  @Override
  public void error(
      final String action,
      final Map<String, String> attributes,
      final Throwable error
  ) {
    mNotifier.error(mPrefix + action, attributes, error);
  }

  /** {@inheritDoc} */
  @Override
  public void error(
      final String action,
      final Map<String, String> attributes
  ) {
    mNotifier.error(mPrefix + action, attributes);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mNotifier.close();
  }
}
