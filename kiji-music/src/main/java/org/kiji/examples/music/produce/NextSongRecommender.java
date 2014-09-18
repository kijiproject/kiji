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


package org.kiji.examples.music.produce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.kiji.mapreduce.KijiProducer;
import org.kiji.mapreduce.ProducerContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreClient;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 *
 */
public class NextSongRecommender extends KijiProducer implements KeyValueStoreClient {

  @Override
  public KijiDataRequest getDataRequest() {
    // Only request the most recent version from the "interactions:track_plays" column.
    return KijiDataRequest.create("interactions", "track_plays");
  }

  @Override
  public String getOutputColumn() {
    return "interactions:next_song_rec";
  }

  @Override
  public void produce(KijiRowData input, ProducerContext context) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    Map<String, KeyValueStore<?,?>> kvStoreBindings = new HashMap<String, KeyValueStore<?,?>>();
    return kvStoreBindings;
  }

}
