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

package org.kiji.delegation;

import java.util.Map;

/**
 * A set of implementations of IFoo.
 */
public final class Providers {
  private Providers() { }

  // Implementations of IFoo

  public static final class ImplA implements IFoo {
    @Override
    public String getMessage() {
      return "implA";
    }
  }

  public static final class ImplB implements IFoo {
    @Override
    public String getMessage() {
      return "implB";
    }
  }

  public static final class ImplC implements IFoo {
    @Override
    public String getMessage() {
      return "implC";
    }
  }

  // Implementations of IPriorityFoo

  public static final class PriorityImplA implements IPriorityFoo {
    @Override
    public String getMessage() {
      return "priorityimplA";
    }

    @Override
    public int getPriority(Map<String, String> runtimeHints) {
      return 50;
    }
  }

  public static final class PriorityImplB implements IPriorityFoo {
    @Override
    public String getMessage() {
      return "priorityimplB";
    }

    @Override
    public int getPriority(Map<String, String> runtimeHints) {
      // This is the best provider in priority mode.
      return 1000;
    }
  }

  public static final class PriorityImplC implements IPriorityFoo {
    @Override
    public String getMessage() {
      return "priorityimplC";
    }

    @Override
    public int getPriority(Map<String, String> runtimeHints) {
      // In priority service mode, this class cannot be used.
      return 0;
    }
  }

  public static final class NamedImplA implements INamedFoo {
    @Override
    public String getMessage() {
      return "NamedA";
    }

    @Override
    public String getName() {
      return "A";
    }
  }

  public static final class NamedImplB implements INamedFoo {
    @Override
    public String getMessage() {
      return "NamedB";
    }

    @Override
    public String getName() {
      return "B";
    }
  }

  ///// providers of IBar; two of them have the same getName(), which is
  ///// nondeterministic.

  public static final class NamedBarA1 implements IBar {
    @Override
    public String getMessage() {
      return "a1";
    }

    @Override
    public String getName() {
      return "A";
    }
  }

  public static final class NamedBarA2 implements IBar {
    @Override
    public String getMessage() {
      return "a2";
    }

    @Override
    public String getName() {
      return "A"; // same as NamedBarA1.
    }
  }

  public static final class NamedBarB implements IBar {
    @Override
    public String getMessage() {
      return "b";
    }

    @Override
    public String getName() {
      return "B";
    }
  }
}
