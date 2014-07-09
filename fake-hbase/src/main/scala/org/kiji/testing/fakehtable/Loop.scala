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

package org.kiji.testing.fakehtable

import scala.util.control.ControlThrowable

/**
 * Unconditional loop with break/continue control statements.
 *
 * Inspired from scala.util.control.Breaks
 */
class Loop {
  /** Exception used to signal a continue. */
  private final val breakException = new BreakControl()

  /** Exception used to signal a continue. */
  private final val continueException = new ContinueControl()

  /**
   * Unconditional loop with break and continue.
   *
   * Most programming languages offer the ability to exit a loop using a break statement, or
   * to skip the remaining of the current iteration using a continue statement.
   * Scala does not, but instead provides a Breakable control structure (built using exceptions).
   * This class intends to fill the gap and implements a control structure, that provides
   * an unconditional loop that supports both break and continue.
   *
   * Example:
   * {{{
   *   val MainLoop = new Loop()
   *   MainLoop {
   *     // Do things...
   *     if (...) MainLoop.break
   *     // Do other things...
   *     if (...) MainLoop.continue
   *     // Do some other things...
   *     // loop over
   *   }
   * }}}
   *
   * @param op The loop body.
   */
  def apply(op: => Unit): Unit = {
    try {
      var loop = true
      while (loop) {
        try {
          op
        } catch {
          case cc: ContinueControl => if (cc != continueException) throw cc // else loop over
        }
      }
    } catch {
      case bc: BreakControl => if (bc != breakException) throw bc // else break this loop
    }
  }

  /** Equivalent of a Java continue. */
  def continue(): Unit = {
    throw continueException
  }

  /** Equivalent of a Java break */
  def break(): Unit = {
    throw breakException
  }
}

/**
 * Default loop instance.
 *
 * You may use this default loop if you don't nest loops and need to break/continue through
 * enclosing loops.
 */
object Loop extends Loop

/** Exception used to signal a continue. */
private class ContinueControl extends ControlThrowable

/** Exception used to signal a break. */
private class BreakControl extends ControlThrowable
