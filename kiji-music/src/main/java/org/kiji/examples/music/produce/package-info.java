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

/**
 * This package contains producers used in the KijiMusic tutorial.
 *
 * NextSongRecommender can be run from the command-line:
 * kiji produce \
 * --producer=org.kiji.examples.music.produce.NextSongRecommender \
 *  --input="format=kiji table=$KIJI/users" \
 *  --output="format=kiji table=$KIJI/users nsplits=2" \
 *  --lib=${LIBS_DIR} \
 *  --kvstores=KVStoreConfig.xml
 */
package org.kiji.examples.music.produce;
