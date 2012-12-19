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

package org.kiji.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>A RecordReader that reads an entire file as a single key-value.
 * The key is the qualified path to this file, and the value is the Text contents of the file.
 * This is intended for reading small files.
 * (Note that the entire file must be able to fit in ram)</p>
 */
public class WholeFileRecordReader extends RecordReader<Text, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(WholeFileRecordReader.class);

  /** The path to the file to read. */
  private final Path mFileToRead;
  /** The length of this file. */
  private final long mFileLength;

  /** The Configuration. */
  private final Configuration mConf;

  /** Whether this FileSplit has been processed. */
  private boolean mProcessed;
  /** Single Text to store the file name of the current file. */
  private final Text mFileName;
  /** Single Text to store the value of this file (the value) when it is read. */
  private final Text mFileText;

  /**
   * Implementation detail: This constructor is built to be called via
   * reflection from within CombineFileRecordReader.
   *
   * @param fileSplit The CombineFileSplit that this will read from.
   * @param context The context for this task.
   * @param pathToProcess The path index from the CombineFileSplit to process in this record.
   */
  public WholeFileRecordReader(CombineFileSplit fileSplit, TaskAttemptContext context,
      Integer pathToProcess) {
    mProcessed = false;
    mFileToRead = fileSplit.getPath(pathToProcess);
    mFileLength = fileSplit.getLength(pathToProcess);
    mConf = context.getConfiguration();

    assert 0 == fileSplit.getOffset(pathToProcess);
    if (LOG.isDebugEnabled()) {
      LOG.debug("FileToRead is: " + mFileToRead.toString());
      LOG.debug("Processing path " + pathToProcess + " out of " + fileSplit.getNumPaths());

      try {
        FileSystem fs = FileSystem.get(mConf);
        assert fs.getFileStatus(mFileToRead).getLen() == mFileLength;
      } catch (IOException ioe) {
        // oh well, I was just testing.
      }
    }

    mFileName = new Text();
    mFileText = new Text();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mFileText.clear();
  }

  /**
   * Returns the absolute path to the current file.
   *
   * @return The absolute path to the current file.
   * @throws IOException never.
   * @throws InterruptedException never.
   */
  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return mFileName;
  }

  /**
   * <p>Returns the current value.  If the file has been read with a call to NextKeyValue(),
   * this returns the contents of the file as a BytesWritable.  Otherwise, it returns an
   * empty BytesWritable.</p>
   *
   * <p>Throws an IllegalStateException if initialize() is not called first.</p>
   *
   * @return A BytesWritable containing the contents of the file to read.
   * @throws IOException never.
   * @throws InterruptedException never.
   */
  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return mFileText;
  }

  /**
   * Returns whether the file has been processed or not.  Since only one record
   * will be generated for a file, progress will be 0.0 if it has not been processed,
   * and 1.0 if it has.
   *
   * @return 0.0 if the file has not been processed.  1.0 if it has.
   * @throws IOException never.
   * @throws InterruptedException never.
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (mProcessed) ? (float) 1.0 : (float) 0.0;
  }

  /**
   * All of the internal state is already set on instantiation.  This is a no-op.
   *
   * @param split The InputSplit to read.  Unused.
   * @param context The context for this task.  Unused.
   * @throws IOException never.
   * @throws InterruptedException never.
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    // no-op.
  }

  /**
   * <p>If the file has not already been read, this reads it into memory, so that a call
   * to getCurrentValue() will return the entire contents of this file as Text,
   * and getCurrentKey() will return the qualified path to this file as Text.  Then, returns
   * true.  If it has already been read, then returns false without updating any internal state.</p>
   *
   * @return Whether the file was read or not.
   * @throws IOException if there is an error reading the file.
   * @throws InterruptedException if there is an error.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!mProcessed) {
      if (mFileLength > (long) Integer.MAX_VALUE) {
        throw new IOException("File is longer than Integer.MAX_VALUE.");
      }
      byte[] contents = new byte[(int) mFileLength];

      FileSystem fs = mFileToRead.getFileSystem(mConf);
      FSDataInputStream in = null;
      try {
        // Set the contents of this file.
        in = fs.open(mFileToRead);
        IOUtils.readFully(in, contents, 0, contents.length);
        mFileText.set(contents, 0, contents.length);

        // Set the name of this file.
        String fileName = mFileToRead.makeQualified(fs).toString();
        mFileName.set(fileName);
      } finally {
        IOUtils.closeStream(in);
      }
      mProcessed = true;
      return true;
    }
    return false;
  }
}
