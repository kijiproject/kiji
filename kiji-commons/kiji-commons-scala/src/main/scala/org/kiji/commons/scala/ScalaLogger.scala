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
package org.kiji.commons.scala

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Scala friendly interface to SLF4J Logger.
 */
class ScalaLogger private (
  log: Logger
) {
  /** Underlying SLF4J logger. */
  private final val mLog: Logger = log

  /**
   * Returns the underlying SLF4J logger.
   *
   * @return the underlying SLF4J logger.
   */
  def getLogger(): Logger = mLog

  def error(msg: => String): Unit = if (isErrorEnabled()) { mLog.error(msg) }
  def warn(msg: => String): Unit = if (isWarnEnabled()) { mLog.warn(msg) }
  def info(msg: => String): Unit = if (isInfoEnabled()) { mLog.info(msg) }
  def debug(msg: => String): Unit = if (isDebugEnabled()) { mLog.debug(msg) }
  def trace(msg: => String): Unit = if(isTraceEnabled()) { mLog.trace(msg) }

  def isErrorEnabled(): Boolean = mLog.isErrorEnabled()
  def isWarnEnabled(): Boolean = mLog.isWarnEnabled()
  def isInfoEnabled(): Boolean = mLog.isInfoEnabled()
  def isDebugEnabled(): Boolean = mLog.isDebugEnabled()
  def isTraceEnabled(): Boolean = mLog.isTraceEnabled()
}

object ScalaLogger {
  def apply(clazz: Class[_]): ScalaLogger = {
    new ScalaLogger(LoggerFactory.getLogger(clazz))
  }
}
