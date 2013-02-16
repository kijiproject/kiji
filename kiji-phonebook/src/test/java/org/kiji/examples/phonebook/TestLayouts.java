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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.shell.api.Client;
import org.kiji.schema.util.Resources;

/**
 * Test that DDL and JSON layouts are correct for this version of KijiSchema.
 */
public class TestLayouts extends KijiClientTest {
  /**
   * Test that DDL command creates the table correctly.
   *
   * @throws IOException if there's a problem interacting w/ KijiURI, etc.
   */
  @Test
  public void testDDLCreate() throws IOException {
    KijiURI uri = getKiji().getURI();
    Client ddlClient = Client.newInstance(uri);
    ddlClient.executeStream(Resources.openSystemResource("phonebook/layout.ddl"));
    System.out.println(ddlClient.getLastOutput());
    ddlClient.close();
  }

  /**
   * Test that the JSON file can be used with kiji create-table.
   *
   * @throws IOException if there's a problem parsing the JSON file or creating the table.
   */
  @Test
  public void testJSONFileIsValid() throws IOException {
    InputStream jsonStream = Resources.openSystemResource("phonebook/layout.json");
    KijiTableLayout layout = KijiTableLayout.createFromEffectiveJson(jsonStream);
    getKiji().createTable("phonebook", layout); // Test that it works...
  }

  /**
   * Test that the JSON file and the DDL create identical tables.
   *
   * @throws IOException if things go wrong with either side of this.
   */
  @Test
  public void testDDLMatchesJSON() throws IOException {
    // Create an instance through the DDL.
    KijiURI uri = getKiji().getURI();
    Client ddlClient = Client.newInstance(uri);
    ddlClient.executeStream(Resources.openSystemResource("phonebook/layout.ddl"));
    System.out.println(ddlClient.getLastOutput());

    KijiTableLayout ddlLayout = getKiji().getMetaTable().getTableLayout("phonebook");
    final String realJsonFromDDL = ddlLayout.getDesc().toString();

    // Now delete the instance we just created...
    ddlClient.executeUpdate("DROP TABLE phonebook");
    ddlClient.close();

    // ... So we can create a new one via the JSON file

    InputStream jsonStream = Resources.openSystemResource("phonebook/layout.json");
    KijiTableLayout layout = KijiTableLayout.createFromEffectiveJson(jsonStream);
    getKiji().createTable("phonebook", layout); // Test that it works...

    // Test that the two have equivalent representations.

    KijiTableLayout retrievedFromJson = getKiji().getMetaTable().getTableLayout("phonebook");
    final String realJsonFromJson = retrievedFromJson.getDesc().toString();

    if (!ddlLayout.equals(retrievedFromJson)) {
      System.out.println("Real JSON for the real layout from the DDL doesn't match the file");
      System.out.println("JSON layout retrieved from DDL statement:");
      System.out.println(realJsonFromDDL);
      System.out.println("JSON layout parsed by reading in layout.json:");
      System.out.println(realJsonFromJson);
      assertEquals("JSON is not equal", realJsonFromDDL, realJsonFromJson);
    }
  }
}
