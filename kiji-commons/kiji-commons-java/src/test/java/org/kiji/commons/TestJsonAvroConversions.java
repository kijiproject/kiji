/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.commons;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import org.kiji.commons.avro.FakeRecord;

public class TestJsonAvroConversions {

  @Test
  public void testPrimitivesFromJson() throws Exception {
    Assert.assertEquals(1, FromJson.fromJsonString("1", Schema.create(Schema.Type.INT)));
    Assert.assertEquals(1L, FromJson.fromJsonString("1", Schema.create(Schema.Type.LONG)));
    Assert.assertEquals(1.0f, FromJson.fromJsonString("1", Schema.create(Schema.Type.FLOAT)));
    Assert.assertEquals(1.0, FromJson.fromJsonString("1", Schema.create(Schema.Type.DOUBLE)));
    Assert.assertEquals(1.0f, FromJson.fromJsonString("1.0", Schema.create(Schema.Type.FLOAT)));
    Assert.assertEquals(1.0, FromJson.fromJsonString("1.0", Schema.create(Schema.Type.DOUBLE)));
    Assert.assertEquals(
        "Text", FromJson.fromJsonString("'Text'", Schema.create(Schema.Type.STRING)));
    Assert.assertEquals(
        "Text", FromJson.fromJsonString("\"Text\"", Schema.create(Schema.Type.STRING)));
    Assert.assertEquals(
        true, FromJson.fromJsonString("true", Schema.create(Schema.Type.BOOLEAN)));
    Assert.assertEquals(
        false, FromJson.fromJsonString("false", Schema.create(Schema.Type.BOOLEAN)));

    FakeRecord record = FakeRecord.newBuilder().setData(1).build();
    Assert.assertEquals(
        record.toString(),
        FromJson.fromJsonNode(
            ToJson.toJsonNode(record, record.getSchema()),
            record.getSchema()
        ).toString()
    );
  }
}
