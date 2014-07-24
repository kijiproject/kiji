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
package org.kiji.scoring;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.scoring.FreshKijiTableReader.FreshRequestOptions;

public class TestFreshRequestOptions {

  private FreshRequestOptions.Builder newBuilder() {
    return FreshRequestOptions.Builder.create();
  }

  @Test
  public void testBuilder() {
    final KijiColumnName infoName = KijiColumnName.create("info", "name");
    final FreshRequestOptions.Builder builder = newBuilder()
        .withParameters(ImmutableMap.of("a", "b"))
        .withTimeout(10)
        .withDisabledColumns(Sets.newHashSet(infoName))
        .addParameters(ImmutableMap.of("c", "d"));
    final FreshRequestOptions options = builder.build();

    Assert.assertEquals(ImmutableMap.of("a", "b", "c", "d"), builder.getParameters());
    Assert.assertEquals(10, builder.getTimeout());
    Assert.assertEquals(Sets.newHashSet(infoName), builder.getDisabledColumns());

    Assert.assertEquals(ImmutableMap.of("a", "b", "c", "d"), options.getParameters());
    Assert.assertEquals(10, options.getTimeout());
    Assert.assertEquals(Sets.newHashSet(infoName), options.getDisabledColumns());
  }
}
