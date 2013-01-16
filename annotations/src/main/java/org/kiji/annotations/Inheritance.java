/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.annotations;

import java.lang.annotation.Documented;

/**
 * Annotations that inform users of an interface or class's intended use
 * case. The class/interface may be {@link Sealed} or {@link Extensible}.
 *
 * <ul>
 *   <li>By default, unlabeled interfaces (or classes) should be assumed to be sealed.</li>
 *   <li>A {@link Sealed} interface should only be implemented by Kiji.
 *     Clients should expect to hold references to concrete instances
 *     of such classes/interfaces, but should not implement them.</li>
 *   <li>An {@link Extensible} interface is intended for extension by clients. You
 *     may write classes that implement these interfaces.</li>
 * </ul>
 *
 * <p>These
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class Inheritance {

  /** This class cannot be constructed. */
  private Inheritance() { }

  /**
   * An interface or class that you should not implement or extend. You may
   * be handed instances of this interface in response to another Kiji API
   * call.
   */
  @Documented public @interface Sealed { }

  /** An interface or class that users can implement or extend. */
  @Documented public @interface Extensible { }
}
