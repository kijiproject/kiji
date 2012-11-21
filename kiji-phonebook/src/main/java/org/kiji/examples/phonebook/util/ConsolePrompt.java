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

package org.kiji.examples.phonebook.util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.IOUtils;

/**
 * A source of input that delivers prompts to the user and reads responses from the console.
 */
public final class ConsolePrompt implements Closeable {

  /** The underlying reader instance. */
  private BufferedReader mConsoleReader;

  /** Initialize a ConsolePrompt instance. */
  public ConsolePrompt() {
    try {
      mConsoleReader = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      // UTF-8 is specified as supported everywhere on Java; should not get here.
      throw new IOError(uee);
    }
  }

  /**
   * Display a prompt and then wait for the user to respond with input.
   *
   * @param prompt the prompt string to deliver.
   * @return the user's response.
   * @throws IOError if there was an error reading from the console.
   */
  public String readLine(String prompt) {
    System.out.print(prompt);

    try {
      return mConsoleReader.readLine();
    } catch (IOException ioe) {
      throw new IOError(ioe);
    }
  }

  /**
   * Close all underlying resources.
   */
  @Override
  public void close() {
    IOUtils.closeQuietly(mConsoleReader);
  }
}
