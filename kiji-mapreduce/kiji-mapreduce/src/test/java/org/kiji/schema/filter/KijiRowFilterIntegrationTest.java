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

package org.kiji.schema.filter;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiAdmin;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * A base class for integration tests of KijiRowFilter implementations.
 */
public abstract class KijiRowFilterIntegrationTest extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(KijiRowFilterIntegrationTest.class);

  /** The name of a test table. */
  private static final String FOODS_TABLE_NAME = "foods";

  /** An opened kiji instance. */
  private Kiji mKiji;

  /** An opened kiji table. */
  private KijiTable mFoodsTable;

  /** A kiji admin to create and delete the food table. */
  private KijiAdmin mAdmin;

  @Before
  public void setup() throws IOException {
    mKiji = Kiji.Factory.open(getKijiConfiguration());
    mAdmin = mKiji.getAdmin();
    try {
      createTable();
    } catch (Exception e) {
      fail(e.getMessage());
    }

    mFoodsTable = mKiji.openTable(FOODS_TABLE_NAME);
  }

  @After
  public void teardown() throws Exception {
    mFoodsTable.close();
    deleteTable();
    // TODO(SCHEMA-158): Clean up internal hbaseAdmin in mAdmin.
    mKiji.close();
  }

  /**
   * Creates a table 'foods'.
   */
  private void createTable() throws Exception {
    final KijiTableLayout foodTableLayout =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.FOODS), null);

    mAdmin.createTable(FOODS_TABLE_NAME, foodTableLayout, false);
    LOG.info("Table " + FOODS_TABLE_NAME + " created.");
  }

  /**
   * Deletes the 'foods' table.
   */
  private void deleteTable() throws Exception {
    mAdmin.deleteTable(FOODS_TABLE_NAME);
  }

  /**
   * Gets the opened kiji instance.
   *
   * @return The opened kiji instance.
   */
  protected Kiji getKiji() {
    return mKiji;
  }

  /**
   * Gets the opened foods table.
   *
   * @return The opened foods table.
   */
  protected KijiTable getFoodsTable() {
    return mFoodsTable;
  }

  /**
   * Writes a food to the 'foods' table.
   *
   * @param foodWriter A writer for the foods table.
   * @param foodName The name of the food to write to the table.
   * @param foodColor The color of the food.
   * @param peopleWhoLikeIt The names of people who like the food.
   * @param countries The list of countries where this food can be purchased.
   * @param costs The list of the costs of this food in each country in countries.
   */
  protected void writeFood(KijiTableWriter foodWriter, String foodName, String foodColor,
      List<String> peopleWhoLikeIt, List<String> countries, List<Double> costs)
      throws IOException, InterruptedException {
    // The size of the countries and costs should match.
    if (countries.size() != costs.size()) {
      LOG.info("Food table not written to because of differing list sizes of countries and costs.");
      return;
    }
    // Define a few Avro schemas we'll be using.
    final Schema stringType = Schema.create(Schema.Type.STRING);
    final Schema arrayOfStringsType = Schema.createArray(stringType);
    final Schema floatType = Schema.create(Schema.Type.DOUBLE);

    // Use the name of the food as the entity id.
    EntityId entityId = mFoodsTable.getEntityId(foodName);

    // Set the name of the food.
    foodWriter.put(entityId, "info", "name", new KijiCell<CharSequence>(stringType, foodName));

    // Set the color of the food.
    foodWriter.put(entityId, "info", "color", new KijiCell<CharSequence>(stringType, foodColor));

    // Set the array of people who like the food (unless nobody likes it).
    List<CharSequence> arrayOfPeopleWhoLikeIt = new ArrayList<CharSequence>();
    for (String personWhoLikesIt : peopleWhoLikeIt) {
      arrayOfPeopleWhoLikeIt.add(personWhoLikesIt);
    }
    if (!arrayOfPeopleWhoLikeIt.isEmpty()) {
      foodWriter.put(entityId, "info", "peopleWhoLikeIt",
          new KijiCell<List<CharSequence>>(arrayOfStringsType, arrayOfPeopleWhoLikeIt));
    }
    // Set the cost by country map family data.
    for (int i = 0; i < costs.size(); i++) {
      foodWriter.put(entityId, "priceByCountry", countries.get(i),
          new KijiCell<Double>(floatType, costs.get(i)));
    }
  }
}
