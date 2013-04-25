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

package org.kiji.mapreduce.lib.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.kiji.mapreduce.lib.avro.Node;

/**
 * For reading an avro container file that contains nodes.  This is not thread-safe.
 */
public class NodeAvroFile extends Configured implements Iterable<Node> {
  /** The path to the avro file. */
  private final Path mPath;
  /** Whether the avro file is open. */
  private boolean mIsOpened;
  /** The file system the avro file lives in. */
  private FileSystem mFileSystem;
  /** The avro file reader for the file. */
  private DataFileStream<Node> mFileReader;

  /**
   * Constructs a node avro file over an existing path.  You must call open() before
   * reading from the file.
   *
   * @param path The path to the avro file.
   * @param conf The configuration specifying the Hadoop cluster.
   */
  public NodeAvroFile(String path, Configuration conf) {
    this(new Path(path), conf);
  }

  /**
   * Constructs a node avro file over an existing path.  You must call open() before
   * reading from the file.
   *
   * @param path The path to the avro file.
   * @param conf The configuration specifying the Hadoop cluster.
   */
  public NodeAvroFile(Path path, Configuration conf) {
    super(conf);
    mPath = path;
    mIsOpened = false;
  }

  /**
   * Opens the avro file for reading or writing.
   *
   * @throws IOException If there is an error opening the file.
   */
  public void open() throws IOException {
    mFileSystem = mPath.getFileSystem(getConf());
    SpecificDatumReader<Node> nodeReader = new SpecificDatumReader<Node>(Node.class);
    mFileReader = new DataFileStream<Node>(mFileSystem.open(mPath), nodeReader);
    mIsOpened = true;
  }

  /**
   * Gets whether the file is currently open.
   *
   * @return whether the file has been opened.
   */
  public boolean isOpened() {
    return mIsOpened;
  }

  /**
   * Closes the avro file.  No more methods may be called after this.
   *
   * @throws IOException If there is an error.
   */
  public void close() throws IOException {
    mFileReader.close();
    mFileReader = null;
    mFileSystem = null;
    mIsOpened = false;
  }

  /**
   * Returns an iterator over the nodes in the file.  You must call open first.
   *
   * @return A node iterator.
   */
  @Override
  public Iterator<Node> iterator() {
    Preconditions.checkState(isOpened());

    return mFileReader.iterator();
  }

  /**
   * Loads the entire file into memory.  You must call open() first.
   *
   * @return The list of nodes in the file.
   * @throws IOException If there is an error reading the nodes from the file.
   */
  public List<Node> load() throws IOException {
    Preconditions.checkState(isOpened());

    List<Node> result = new ArrayList<Node>();
    for (Node node : this) {
      result.add(node);
    }
    return result;
  }
}
