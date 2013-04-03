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

package org.kiji.mapreduce.input.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for formatted XML files.  XML files are read
 * line by line searching for record start and end tags.  Keys are the byte offset in the file of
 * the start of record opening tags. Values are the contents of the file between start and end tags
 * (inclusive). A record will contain exactly <code>&lt;record&gt;contents&lt;/record&gt;</code>
 * with any preceding or trailing whitespace removed.  Comments and CDATA containing record tags
 * will be read as valid tags and may cause the record reader to return invalid records.
 *
 * This Input format can be used with the stock
 * {@link org.kiji.mapreduce.lib.bulkimport.XMLBulkImporter}.
 */
@ApiAudience.Private
public final class XMLInputFormat extends FileInputFormat<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLInputFormat.class);

  /** Configuration key for XML tag to start and end records. */
  public static final String RECORD_TAG_CONF_KEY = "kiji.input.xml.record.tag";
  /** Configuration key for XML version and encoding information. */
  public static final String XML_HEADER_CONF_KEY = "kiji.input.xml.header";
  /**
   * Configuration key for setting the maximum number of bytes the record reader may read beyond
   * the end of a split.  Default value is equal to the size of the split.
   */
  public static final String XML_OVERRUN_CONF_KEY = "kiji.input.xml.overrun.allowance";

  /** {@inheritDoc} */
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    XMLRecordReader reader = new XMLRecordReader();
    reader.initialize(split, context);
    return reader;
  }

  /**
   * <p>
   * A {@link org.apache.hadoop.mapreduce.RecordReader} for parsing XML records.  Seeks until it
   * finds the user specified start tag, captures until it finds the user specified end tag, and
   * returns a <code>Text</code> object containing an entire XML record.
   * </p>
   * <p>
   * XMLRecordReader is package private for testing purposes only and should not be accessed
   * externally.
   * </p>
   */
  @ApiAudience.Private
  static final class XMLRecordReader extends RecordReader<LongWritable, Text> {
    private static final String DEFAULT_XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    /** The byte offset within the file of the start of the current file split. */
    private long mStartOffset;
    /** The byte offset within the file of the end of the current file split (exclusive). */
    private long mEndOffset;
    /** The current byte offset within the file. */
    private long mCurrentOffset;
    /**
     * The maximum bytes the reader may read beyond the end of the split when searching for the
     * end of a record.
     */
    private long mOverrunAllowance;
    /** A line reader for the input file. */
    private BufferedReader mReader;
    /** Byte offset of the current record. */
    private LongWritable mCurrentKey;
    /** Value of the current record. */
    private Text mCurrentValue;
    /** Tag that marks the beginning of a record. */
    private char[] mRecordBeginChars;
    /** Tag that marks the end of a record. */
    private char[] mRecordEndChars;
    /** XML header for each record. */
    private String mHeader;
    /** StringBuilder holding partial record. */
    private StringBuilder mRecordBuilder;

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      FileSplit fileSplit = downcast(split);
      Configuration conf = context.getConfiguration();

      // Initialize the start and end offsets for the current split.
      mStartOffset = fileSplit.getStart();
      mEndOffset = mStartOffset + fileSplit.getLength();
      mOverrunAllowance = conf.getLong(XML_OVERRUN_CONF_KEY, fileSplit.getLength());

      // Open the file containing the input split.
      FileSystem fileSystem = fileSplit.getPath().getFileSystem(conf);
      FSDataInputStream fileInputStream = fileSystem.open(fileSplit.getPath());

      // Seek to the beginning of the input split.
      fileInputStream.seek(mStartOffset);
      mCurrentOffset = mStartOffset;
      mReader = new BufferedReader(new InputStreamReader(fileInputStream, "utf-8"));

      // Initialize Key and Value.
      mCurrentKey = new LongWritable();
      mCurrentValue = new Text();

      // Set begin and end tag values.
      mRecordBeginChars = String.format("<%s", Preconditions.checkNotNull(
          conf.get(RECORD_TAG_CONF_KEY), "Record tag may not be null.  Specify a record tag in "
          + "the configuration with key: " + RECORD_TAG_CONF_KEY)).toCharArray();
      mRecordEndChars = String.format("</%s>", Preconditions.checkNotNull(
          conf.get(RECORD_TAG_CONF_KEY), "Record tag may not be null.  Specify a record tag in "
          + "the configuration with key: " + RECORD_TAG_CONF_KEY)).toCharArray();

      mHeader = conf.get(XML_HEADER_CONF_KEY, DEFAULT_XML_HEADER);
    }

    /**
     * Downcast an InputSplit to a FileSplit.
     *
     * @param split The InputSplit.
     * @return The FileSplit.
     */
    private FileSplit downcast(InputSplit split) {
      Preconditions.checkArgument(split instanceof FileSplit,
          String.format("Only %s is supported, but found %s", FileSplit.class, split.getClass()));
      return (FileSplit) split;
    }

    /**
     * Find the start of a record given the current field values.
     *
     * @return Whether the beginning of a record was found.
     * @throws IOException in case of an IO error.
     */
    private boolean findRecordStart() throws IOException {
      return findRecordStart(
          mRecordBeginChars,
          mStartOffset,
          mEndOffset,
          mReader,
          mCurrentKey,
          mRecordBuilder
      );
    }

    /**
     * Seeks into a split until it finds a given record delimiting start string.
     * Package private for testing purposes only, should not be called externally.
     *
     * @param recordBeginChars A char array of the record delimiting tag.  This consists of the
     *   opening &lt; character for the tag and the name of the entity itself; it does not include
     *   the trailing &gt; character because a tag may contain attributes.  For example, for a
     *   record consisting of <tt>&lt;user&gt;contents&lt;user&gt;</tt>, this will contain an array
     *   of characters representing "&lt;user".
     * @param startOffset Byte offset in the file of the beginning of the split.
     * @param endOffset Byte offset in the file of the end of the split.
     * @param reader BufferedReader on the input data.
     * @param currentKey A LongWritable to be set to the start of a found record.
     * @param recordBuilder A StringBuilder containing the partially formed record.  The record
     *   start tag will be appended to the StringBuilder if this method find the start of a record
     *   (i.e. returns true).
     * @return True if the beginning of a record has been found, false if it reaches the end of the
     *   file.
     * @throws IOException in case of an IO error.
     */
    boolean findRecordStart(
        char[] recordBeginChars,
        long startOffset,
        long endOffset,
        BufferedReader reader,
        LongWritable currentKey,
        StringBuilder recordBuilder
        ) throws IOException {
      final int recordBeginLength = recordBeginChars.length;
      // Have we found the start of a record?
      boolean foundBeginTag = false;
      // Index of the next unmatched character in mRecordBeginChars.
      int matchRecordBeginIndex = 0;
      // Next record character to match.
      char nextCharToMatch = recordBeginChars[matchRecordBeginIndex];

      // Seek until you find a record begin tag.
      while (!foundBeginTag) {
        // Break if past the end of the split only if a record has not been partially matched.
        // Protects against the case that a record begin tag falls across a split boundary.
        if (matchRecordBeginIndex == 0 && mCurrentOffset >= endOffset) {
          return false;
        }
        // Read the next char.
        int nextChar = reader.read();
        // Return false if we reach EOF without opening a record.
        if (nextChar == -1) {
          return false;
        }
        final char currentChar = (char) nextChar;
        mCurrentOffset++;
        // If the next char is next in the record begin tag, increment the match index.
        if (currentChar == nextCharToMatch) {
          matchRecordBeginIndex++;
          // If we've matched the entire record begin tag, match against '>' and whitespace.
          if (matchRecordBeginIndex == recordBeginLength) {
            nextChar = reader.read();
            // Return false if we reach EOF without opening a record.
            if (nextChar == -1) {
              return false;
            }
            final char recordValidatorChar = (char) nextChar;
            mCurrentOffset++;
            // If the character following the begin tag is valid, save it and flag a record start.
            if (recordValidatorChar == '>' || Character.isWhitespace(recordValidatorChar)) {
              foundBeginTag = true;
              // Set the current key to the beginning of the record begin tag.  Value is the
              // current location in the file, minus the length of the record begin tag minus one
              // for the extra character following the record begin tag.
              currentKey.set(startOffset + mCurrentOffset - recordBeginLength - 1);
              // Add the record begin tag to the StringBuilder holding the partial record.
              recordBuilder.append(recordBeginChars).append(recordValidatorChar);
            } else {
              // If we have matched the entire begin tag, but it is followed by an invalid char,
              // reset the matcher.  For example, if you are searching for records beginning with
              // "<foo", "<food>" should not match because 'd' is neither '>' nor whitespace.
              matchRecordBeginIndex = 0;
              nextCharToMatch = recordBeginChars[matchRecordBeginIndex];
            }
          } else {
            // If we haven't matched the entire record begin tag, increment the match char.
            nextCharToMatch = recordBeginChars[matchRecordBeginIndex];
          }
        // If the next char is not next in the record begin tag, reset the matcher.
        } else {
          matchRecordBeginIndex = 0;
          nextCharToMatch = recordBeginChars[matchRecordBeginIndex];
        }
      }
      return foundBeginTag;
    }

    /**
     * Find the end of a record using the current field values.
     *
     * @return True if the end of a record is found, false if the reader reaches the end of the
     *   file.
     * @throws IOException in case of an IO error.
     */
    private boolean findRecordEnd() throws IOException {
      return findRecordEnd(
          mRecordEndChars,
          mReader,
          mEndOffset,
          mOverrunAllowance,
          mRecordBuilder,
          mCurrentValue
      );
    }

    /**
     * Seeks into a split until it finds a given record delimiting end string.
     * Package private for testing purposes only, should not be called externally.
     *
     * @param recordEndChars A char array of the record delimiting tag.  For example, a user record
     *   should end with <tt>&lt/;user&gt;</tt>.
     * @param endOffset Byte offset of the end of the split.
     * @param overrunAllowance Number of bytes beyond the end of the split the reader may look for
     *  the end of an open record.
     * @param reader BufferedReader on the input data.
     * @param currentValue A Text to be set to the contents of an entire record.
     * @param recordBuilder A StringBuilder containing the partially formed record.  Each character
     *   read by the BufferedReader will be appended to the StringBuilder.
     * @return True if the end of a record has been found, false if it reaches the end of the
     *   file or exceeds the overrun allowance.
     * @throws IOException in case of an IO error.
     */
    boolean findRecordEnd(
        char[] recordEndChars,
        BufferedReader reader,
        long endOffset,
        long overrunAllowance,
        StringBuilder recordBuilder,
        Text currentValue
        ) throws IOException {
      final int recordEndLength = recordEndChars.length;
      // Record tag matcher.
      // Index of the next unmatched character in mRecordEndChars
      int matchRecordEndIndex = 0;
      // Next record character to match.
      char nextCharToMatch = recordEndChars[matchRecordEndIndex];

      // Seek until you find a record end tag or exceed the split overrun allowance.
      while (mCurrentOffset <= endOffset + overrunAllowance) {
        // Read the next char and add it to the record output.
        final int nextChar = reader.read();
        // Return false if we reach EOF without closing the record.
        if (nextChar == -1) {
          return false;
        }
        final char currentChar = (char) nextChar;
        mCurrentOffset++;
        recordBuilder.append(currentChar);
        // If the next char is next in the record end tag, increment the matcher.
        if (currentChar == nextCharToMatch) {
          matchRecordEndIndex++;
          // If we've matched the entire record end tag, set the currentValue and return.
          if (matchRecordEndIndex == recordEndLength) {
            currentValue.set(recordBuilder.toString());
            return true;
          } else {
            // If we haven't matched the entire record end tag, increment the matcher.
            nextCharToMatch = recordEndChars[matchRecordEndIndex];
          }
        } else {
          //If the next char is not next in the record end tag, reset the matcher.
          matchRecordEndIndex = 0;
          nextCharToMatch = recordEndChars[matchRecordEndIndex];
        }
      }
      // If we exceed the overrunAllowance, return false.
      return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      mRecordBuilder = new StringBuilder().append(mHeader);
      return findRecordStart() && findRecordEnd();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
      return mCurrentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return mCurrentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      assert mEndOffset > mStartOffset;

      final long bytesTotal = mEndOffset - mStartOffset;
      final long bytesProcessed = Math.max(0L, mCurrentOffset - mStartOffset);

      return (float) bytesProcessed / (float) bytesTotal;
    }

    @Override
    public void close() throws IOException {
      mReader.close();
    }

  }
}
