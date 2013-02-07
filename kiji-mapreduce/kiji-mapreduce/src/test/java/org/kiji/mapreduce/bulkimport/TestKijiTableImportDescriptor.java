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

package org.kiji.mapreduce.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.TestingResources;
import org.kiji.mapreduce.avro.TableImportDescriptorDesc;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.FromJson;

/** Unit tests. */
public class TestKijiTableImportDescriptor {
  public static final String FOO_IMPORT_DESCRIPTOR =
      "org/kiji/mapreduce/mapping/foo-test-import-descriptor.json";

  public static final String FOO_INVALID_DESCRIPTOR =
      "org/kiji/mapreduce/mapping/foo-test-invalid.json";

  @Test
  public void testFoo() throws IOException {
    final String json = TestingResources.get(FOO_IMPORT_DESCRIPTOR);
    TableImportDescriptorDesc mappingDesc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);
    KijiTableImportDescriptor mapping = new KijiTableImportDescriptor(mappingDesc);
    assertEquals("foo", mapping.getName());
    assertEquals(4, mapping.getColumnNameSourceMap().size());
  }

  @Test
  public void testValidation() throws IOException {
    final String json = TestingResources.get(FOO_IMPORT_DESCRIPTOR);

    TableImportDescriptorDesc mappingDesc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);

    KijiTableImportDescriptor mapping = new KijiTableImportDescriptor(mappingDesc);
    final KijiTableLayout fooLayout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());
    mapping.validateDestination(fooLayout);
  }

  @Test
  public void testValidationFail() throws IOException {
    final String json = TestingResources.get(FOO_INVALID_DESCRIPTOR);

    TableImportDescriptorDesc mappingDesc =
        (TableImportDescriptorDesc) FromJson.fromJsonString(json,
            TableImportDescriptorDesc.SCHEMA$);

    KijiTableImportDescriptor mapping = new KijiTableImportDescriptor(mappingDesc);
    final KijiTableLayout fooLayout =
        KijiTableLayout.newLayout(KijiMRTestLayouts.getTestLayout());
    try {
      mapping.validateDestination(fooLayout);
      fail("Should've gotten an InvalidTableImportDescription by here.");
    } catch (InvalidTableImportDescriptorException ie) {
      assertEquals("Table 'test' does not contain column 'info:first_name_nonexistant'.",
          ie.getMessage());
    }
  }
}
