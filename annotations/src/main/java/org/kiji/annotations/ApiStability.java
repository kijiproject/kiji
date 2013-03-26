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
 * Annotations that inform users of a package, class or method's current stability
 * level. The stability level may be {@link Stable}, {@link Evolving}, or {@link Unstable}.
 *
 * <ul>
 *   <li>By default, unlabeled classes should be assumed to be Experimental.</li>
 *   <li>{@link Stable} APIs are guaranteed to change only in binary-compatible
 *     ways within a major version (e.g., all 1.x.x versions)</li>
 *   <li>{@link Evolving} APIs may change in binary-incompatible ways between
 *     minor versions (e.g., from 1.4.1 to 1.5.0) of the software. In
 *     particular, new features may be added in one minor version and altered
 *     or removed in the next.</li>
 *   <li>{@link Experimental} APIs may change at any time.</li>
 *   <li>{@link Unstable} This annotation is no longer used. New code should
 *     use Experimental instead.</li>
 * </ul>
 *
 * <p>{@link Private} APIs should all be considered {@link Unstable}.</p>
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class ApiStability {

  /** This class cannot be constructed. */
  private ApiStability() { }


  /** An API that does not change in incompatible ways within a major version. */
  @Documented public @interface Stable { }

  /** An API that does not change in incompatible ways within a minor version. */
  @Documented public @interface Evolving { }

  /** An API that may change at any time. */
  @Documented public @interface Experimental { }

  /** This annotation is deprecated and {@link Experimental} should be used instead. */
  @Documented @Deprecated public @interface Unstable { }
}
