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

package org.kiji.rest.core;

import static com.yammer.dropwizard.testing.JsonHelpers.asJson;
import static com.yammer.dropwizard.testing.JsonHelpers.jsonFixture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

import org.kiji.rest.representations.KijiRestCell;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCell;

/**
 * Simple test class to ensure that KijiCells are serialized to JSON.
 *
 */
public class TestKijiRestCell {

  @Test
  public void shouldSerializeLongCellToJson() throws Exception {
    DecodedCell<Long> rawCell = new DecodedCell<Long>(null, 1234L);
    KijiCell<Long> cell = new KijiCell<Long>("user", "purchases", 1000L, rawCell);
    KijiRestCell restCell = new KijiRestCell(cell);
    assertThat("a KijiCell can be serialized to JSON", asJson(restCell),
        is(equalTo(jsonFixture("org/kiji/rest/core/fixtures/LongKijiCell.json"))));
  }

  @Test
  public void shouldSerializeStringCellToJson() throws Exception {
    KijiCell<String> cell = new KijiCell<String>("user", "name", 1000L,
        new DecodedCell<String>(null, "a_name"));
    KijiRestCell restCell = new KijiRestCell(cell);
    assertThat("a KijiCell can be serialized to JSON", asJson(restCell),
        is(equalTo(jsonFixture("org/kiji/rest/core/fixtures/StringKijiCell.json"))));
  }
}
