package org.kiji.rest.core;

import static com.yammer.dropwizard.testing.JsonHelpers.asJson;
import static com.yammer.dropwizard.testing.JsonHelpers.jsonFixture;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

import org.kiji.schema.DecodedCell;
import org.kiji.schema.KijiCell;

public class KijiRestCellTest {

  @Test
  public void shouldSerializeLongCellToJson() throws Exception {
    DecodedCell<Long> rawCell = new DecodedCell<Long>(null, 1234l);
    KijiCell<Long> cell = new KijiCell<Long>("user", "purchases", 1000l, rawCell);
    KijiRestCell restCell = new KijiRestCell(cell);
    assertThat("a KijiCell can be serialized to JSON", asJson(restCell),
        is(equalTo(jsonFixture("org/kiji/rest/core/fixtures/LongKijiCell.json"))));
  }

  @Test
  public void shouldSerializeStringCellToJson() throws Exception {
    KijiCell<String> cell = new KijiCell<String>("user", "name", 1000l,
        new DecodedCell<String>(null, "a_name"));
    KijiRestCell restCell = new KijiRestCell(cell);
    assertThat("a KijiCell can be serialized to JSON", asJson(restCell),
        is(equalTo(jsonFixture("org/kiji/rest/core/fixtures/StringKijiCell.json"))));
  }
}
