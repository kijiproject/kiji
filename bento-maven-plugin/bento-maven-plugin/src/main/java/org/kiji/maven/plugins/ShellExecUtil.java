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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
  public static ShellResult executeCommand(
      final String command
  ) throws IOException, ExecutionException {
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    final Process commandProcess;
    try {
      commandProcess = Runtime.getRuntime().exec(command);
    } catch (final IOException ioe) {
      throw new IOException(String.format("Failed to exec: %s", command), ioe);
    }

    final InputStreamReaderCallable stdoutCallable =
        new InputStreamReaderCallable(commandProcess.getInputStream());
    final InputStreamReaderCallable stderrCallable =
        new InputStreamReaderCallable(commandProcess.getErrorStream());
    final Future<String> stdoutFuture = executor.submit(stdoutCallable);
    final Future<String> stderrFuture = executor.submit(stderrCallable);

    // This code has been copied/adapted from Uninterruptibles#getUninterruptibly.
    boolean interrupted = false;
    int exitCode;
    try {
      while (true) {
        try {
          exitCode = commandProcess.waitFor();

          return new ShellResult(
              Uninterruptibles.getUninterruptibly(stderrFuture),
              Uninterruptibles.getUninterruptibly(stdoutFuture),
              exitCode
          );
        } catch (final InterruptedException ise) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Callable that reads an input stream.
   */
  public static class InputStreamReaderCallable implements Callable<String> {
    /**
     * Input stream to read.
     */
    private final InputStream mInputStream;

    /**
     * Construct new callable that reads an input stream.
     *
     * @param inputStream The stream to read.
     */
    public InputStreamReaderCallable(final InputStream inputStream) {
      mInputStream = inputStream;
    }

    /** {@inheritDoc} */
    @Override
    public String call() throws Exception {
      return readInputStream(mInputStream);
    }
  }

  /**
   * Reads an input stream as a string.
   *
   * @param inputStream to read.
   * @return the contents of the input stream as a string.
   * @throws IOException if there is an error reading the stream.
   */
  private static String readInputStream(final InputStream inputStream) throws IOException {
    final StringBuilder outputBuilder = new StringBuilder();
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
    try {
      String outputLine = reader.readLine();
      while (outputLine != null) {
        outputBuilder.append(outputLine).append(System.getProperty("line.separator"));
        outputLine = reader.readLine();
      }
    } finally {
      reader.close();
    }

    return outputBuilder.toString();
  }
}
