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

package org.kiji.maven.plugins;

import com.google.common.base.Objects;

/**
 * Record representing the result of running a shell command as a subprocess.
 */
public final class ShellResult {
  /**
   * Stderr resulting from a process.
   */
  private final String mStderr;

  /**
   * Stdout resulting from a process.
   */
  private final String mStdout;

  /**
   * Exit code resulting from a process.
   */
  private final int mExitCode;

  /**
   * Constructs a shell result.
   *
   * @param stderr resulting from a process.
   * @param stdout resulting from a process.
   * @param exitCode resulting from a process.
   */
  public ShellResult(final String stderr, final String stdout, final int exitCode) {
    mStderr = stderr;
    mStdout = stdout;
    mExitCode = exitCode;
  }

  /**
   * Returns the stderr resulting from the process execution.
   *
   * @return the stderr resulting from the process execution.
   */
  public String getStderr() {
    return mStderr;
  }

  /**
   * Returns the stdout resulting from the process execution.
   *
   * @return the stdout resulting from the process execution.
   */
  public String getStdout() {
    return mStdout;
  }

  /**
   * Returns the exit code resulting from the process execution.
   *
   * @return the exit code resulting from the process execution.
   */
  public int getExitCode() {
    return mExitCode;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("stderr", mStderr)
        .add("stdout", mStdout)
        .add("exitCode", mExitCode)
        .toString();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ShellResult that = (ShellResult) o;

    return Objects.equal(this.mStderr, that.mStderr)
        && Objects.equal(this.mStdout, that.mStdout)
        && Objects.equal(this.mExitCode, that.mExitCode);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hashCode(mStderr, mStdout, mExitCode);
  }
}
