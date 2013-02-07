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

package org.kiji.mapreduce.lib.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiRowData;

/**
 * Utility class containing constants for resources and validation methods for bulk importer tests.
 */
public final class BulkImporterTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BulkImporterTestUtils.class);

  public static final String CSV_IMPORT_DATA =
      "org/kiji/mapreduce/lib/mapping/TestCSVImportInput.txt";

  public static final String TSV_IMPORT_DATA =
      "org/kiji/mapreduce/lib/mapping/TestTSVImportInput.txt";

  public static final String HEADERLESS_CSV_IMPORT_DATA =
      "org/kiji/mapreduce/lib/mapping/HeaderlessCSVImportInput.txt";

  public static final String LOCAL_RESOURCE_PREFIX = "src/test/resources/";

  public static final String FOO_IMPORT_DESCRIPTOR =
    "org/kiji/mapreduce/lib/mapping/foo-test-import-descriptor.json";

  public static String localResource(String resource) {
    return LOCAL_RESOURCE_PREFIX + resource;
  }

  /** No constructor for this utility class. */
  private BulkImporterTestUtils() {}

  /**
   * Validates that the imported rows match up with the expected results.  The imported KijiRowData
   * can come from a KijiRowScanner.
   *
   * @param importedKijiRowData an iterable of KijiRowData whose contents to validate.
   * @throws IOException if there's an error reading the row data
   */
  public static void validateImportedRows(Iterable<KijiRowData> importedKijiRowData)
      throws IOException {
    long rowsProcessed = 0;
    for (KijiRowData row : importedKijiRowData) {
      final EntityId eid = row.getEntityId();
      final String rowId = Bytes.toString(eid.getKijiRowKey());

      final String cellContent = row.getMostRecentValues("info").toString();
      LOG.info("Row: {}, fields: {}", rowId, cellContent);

      // Validate that this row matches the data
      if (rowId.equals("Bob")) {
        assertEquals("Bob", row.getMostRecentValue("info", "first_name").toString());
        assertEquals("Jones", row.getMostRecentValue("info", "last_name").toString());
        assertEquals("bobbyj@aol.com", row.getMostRecentValue("info", "email").toString());
        assertEquals("415-555-4161", row.getMostRecentValue("info", "phone").toString());
      } else if (rowId.equals("Alice")) {
        assertEquals("Alice", row.getMostRecentValue("info", "first_name").toString());
        assertEquals("Smith", row.getMostRecentValue("info", "last_name").toString());
        assertEquals("alice.smith@yahoo.com", row.getMostRecentValue("info", "email").toString());
        assertNull(row.getMostRecentValue("info", "phone"));
      } else if (rowId.equals("John")) {
        assertEquals("John", row.getMostRecentValue("info", "first_name").toString());
        assertEquals("Doe", row.getMostRecentValue("info", "last_name").toString());
        assertEquals("johndoe@gmail.com", row.getMostRecentValue("info", "email").toString());
        assertEquals("202-555-9876", row.getMostRecentValue("info", "phone").toString());
      } else {
        fail("Found an unexpected row: " + rowId);
      }
      rowsProcessed++;
    }
    assertEquals("Rows processed ", 3L, rowsProcessed);
  }
}
