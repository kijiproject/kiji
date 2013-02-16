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

package org.kiji.examples.phonebook;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.mapreduce.KijiConfKeys;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.schema.util.Resources;

/** Tests PhonebookImporter. */
public class IntegrationTestPhonebookImporter
    extends AbstractKijiIntegrationTest {

  private Configuration mConf = null;
  private FileSystem mFS = null;
  private Path mInputPath = null;
  private Kiji mKiji = null;
  private KijiTable mOutputTable = null;

  /**
   * Generates a random HDFS path.
   *
   * @param prefix Prefix for the random file name.
   * @return a random HDFS path.
   * @throws Exception on error.
   */
  private Path makeRandomHdfsPath(String prefix) throws Exception {
    Preconditions.checkNotNull(mFS);
    final Path base = new Path(FileSystem.getDefaultUri(mConf));
    final Random random = new Random(System.nanoTime());
    return new Path(base, String.format("/%s-%s", prefix, random.nextLong()));
  }

  private void writeTestResource(Path path, String testResource) throws Exception {
    final OutputStream ostream = mFS.create(path);
    final InputStream istream = Resources.openSystemResource(testResource);
    IOUtils.copy(istream, ostream);
    istream.close();
    ostream.close();
  }

  private void createPhonebookTable() throws Exception {
    InputStream jsonStream = Resources.openSystemResource("phonebook/layout.json");
    KijiTableLayout layout = KijiTableLayout.createFromEffectiveJson(jsonStream);
    mKiji.createTable("phonebook", layout);
  }

  @Before
  public void setUp() throws Exception {
    mConf = createConfiguration();
    mFS = FileSystem.get(mConf);
    mInputPath = makeRandomHdfsPath("phonebook-input");
    writeTestResource(mInputPath, "phonebook/input-data.txt");

    mKiji = Kiji.Factory.open(getKijiURI(), mConf);

    createPhonebookTable();

    mOutputTable = mKiji.openTable("phonebook");
  }

  @After
  public void tearDown() throws Exception {
    mOutputTable.close();
    mKiji.release();
    mFS.delete(mInputPath, false);

    mOutputTable = null;
    mKiji = null;
    mInputPath = null;
    mFS = null;
    mConf = null;
  }

  @Test
  public void testPhonebookImporter() throws Exception {
    PhonebookImporter importer = new PhonebookImporter();
    importer.setConf(mConf);

    Job job = new Job(mConf);

    importer.configureJob(job, mInputPath);

    // Override the output table uri in the job with one associated with this
    // test-specific Kiji instance.
    final KijiURI tableURI = KijiURI.newBuilder(getKijiURI()).withTableName("phonebook").build();
    job.getConfiguration().set(KijiConfKeys.OUTPUT_KIJI_TABLE_URI, tableURI.toString());

    final boolean jobSuccess = job.waitForCompletion(true);
    assertTrue("Importer exited with non-zero status", jobSuccess);

    checkOutputTable();
  }

  @Test
  public void testStandaloneImporter() throws Exception {
    StandalonePhonebookImporter importer = new StandalonePhonebookImporter();

    File inputFile = File.createTempFile("input", "txt", null);
    inputFile.deleteOnExit();
    final OutputStream ostream = new FileOutputStream(inputFile);
    final InputStream istream = Resources.openSystemResource("phonebook/input-data.txt");
    IOUtils.copy(istream, ostream);
    istream.close();
    ostream.close();

    importer.setConf(mConf);
    importer.setKijiURI(getKijiURI());

    String[] args = { inputFile.getAbsolutePath() };
    int ret = importer.run(args);
    assertEquals("Return code from standalone importer non-zero!", 0, ret);

    checkOutputTable();
  }

  /**
   * Check that the output table was properly populated w/ phonebook entries.
   *
   * @throws IOException if there's an error reading from the table.
   */
  private void checkOutputTable() throws IOException {
    final KijiTableReader reader = mOutputTable.openTableReader();
    final KijiDataRequest kdr = KijiDataRequest.create("info", "firstname");
    KijiRowScanner scanner = reader.getScanner(kdr);
    Set<String> actual = new HashSet<String>();
    for (KijiRowData row : scanner) {
      actual.add(row.getMostRecentValue("info", "firstname").toString());
    }
    scanner.close();
    reader.close();

    Set<String> expected = new HashSet<String>();
    expected.add("Aaron");
    expected.add("John");
    expected.add("Alice");
    expected.add("Bob");

    assertEquals("Output data doesn't match expected results", expected, actual);
  }
}
