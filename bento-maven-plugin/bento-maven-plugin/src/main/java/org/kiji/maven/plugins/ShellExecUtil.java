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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Utility class to containing methods to execute shell commands.
 */
public final class ShellExecUtil {
  /** Disable default constructor. */
  private ShellExecUtil() {}

  /**
   * Execute a shell command, uninterruptibly wait for completion, and return the stdout.
   *
   * Note: The usual solution to execute shell commands from the JVM is as follows:
   * <code>
   * Process process = Runtime.getRuntime().exec(command);
   * process.waitFor(); // Wait for stdout from the program.
   * </code>
   *
   * However, Maven interrupts all process waits/sleeps/etc. in [[LifecyclePhase
   * .POST_INTEGRATION_TEST]]. The rationale for this is probably that Maven builds should not be
   * blocked during cleanup phases. In order to avoid this interference when running commands
   * against the bento script, we uninterruptibly wait for a process completion.
   *
   * @param command to execute.
   * @return stdout.
   * @throws IOException if the command execution encounters and I/O failure.
   * @throws ExecutionException if process future can be uninterruptibly acquired.
   */
  public static String executeCommand(String command) throws IOException, ExecutionException {
    return ProcessWaitForCallable.executeCommand(command);
  }

  /**
   * Execute shell command and return the child process handle.
   *
   * @param command to execute.
   * @return child process handle.
   * @throws IOException if the command execution encounters and I/O failure.
   */
  public static Process executeProcess(String command) throws IOException {
    return Runtime.getRuntime().exec(command);
  }

  /**
   * Execute a process and return its stdout.
   */
  public static class ProcessWaitForCallable implements Callable<String> {
    /** Proces handle. */
    private Process mProcess;

    /**
     * Construct process from shell command and run it.
     *
     * @param command to execute.
     * @throws IOException if running the command fails.
     */
    public ProcessWaitForCallable(String command) throws IOException {
      this.mProcess = Runtime.getRuntime().exec(command);
    }

    /**
     * Wait for shell command to finish and return it's stdout.
     *
     * @return standard out.
     * @throws Exception if the process wait fails or the standard out can not be collected.
     */
    @Override
    public String call() throws Exception {
      mProcess.waitFor();
      BufferedReader stdOutReader = null;
      try {
        stdOutReader = new BufferedReader(
            new InputStreamReader(mProcess.getInputStream(), Charset.defaultCharset()));
        final StringBuffer stdOut = new StringBuffer();
        String stdOutLine = stdOutReader.readLine();
        while (stdOutLine != null) {
          stdOut.append(stdOutLine + "\n");
          stdOutLine = stdOutReader.readLine();
        }
        return stdOut.toString();
      } finally {
        if (null != stdOutReader) {
          stdOutReader.close();
        }
      }
    }

    /**
     * Execute a shell command, uninterruptibly wait for completion, and return the stdout.
     *
     * @param command to execute.
     * @return standard out.
     * @throws IOException if running command fails.
     * @throws ExecutionException if process future can be uninterruptibly acquired.
     */
    public static String executeCommand(String command) throws IOException, ExecutionException {
      FutureTask<String> processFuture =
          new FutureTask<String>(new ProcessWaitForCallable(command));
      ExecutorService executor = Executors.newFixedThreadPool(1);
      executor.execute(processFuture);
      return Uninterruptibles.getUninterruptibly(processFuture);
    }
  }
}
