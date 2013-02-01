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

package org.kiji.mapreduce.lib.map;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiMapper;

/**
 * A Mapper that takes name/text records, and outputs name/flattened-text pairs.
 * Here, "flattened-text" refers to the original text with all newlines
 * replaced by single spaces.
 */
public class TextFlatteningMapper extends KijiMapper<Text, Text, Text, Text> {
  /** Reusable Text to store the contents of each file to write. */
  private Text mFlattenedFile;

  /**
   * Prepares internal state for executing map tasks.
   *
   * @param context The Context to read.  Unused.
   */
  @Override
  protected void setup(Context context) {
    mFlattenedFile = new Text();
  }

  /**
   * Converts The Bytes stored in fileContents to a single String with all new-line
   * characters removed.
   *
   * @param fileName The qualified path to this file.
   * @param fileContents The file to convert, encoded in UTF8.
   * @param context The Context to write to.
   * @throws IOException if there is an error.
   * @throws InterruptedException if there is an error.
   */
  @Override
  protected void map(Text fileName, Text fileContents, Context context)
      throws IOException, InterruptedException {
    // Run over file and remove each newline character.
    // These files are expected to be small (and already fit in a Text object)
    // so we should be able to toString() them.
    String text = fileContents.toString();

    // Replace all newlines with spaces.
    String withoutNewlines = text.replaceAll("\n", " ");
    mFlattenedFile.set(withoutNewlines);

    context.write(fileName, mFlattenedFile);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}
