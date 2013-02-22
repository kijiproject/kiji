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
 * This package contains various gatherers used in the KijiMusic tutorial.
 *
 * Gatherers can be run from the shell:
 * <pre>
 *   kiji gather \
 *   --gatherer=org.kiji.examples.music.gather.SongPlayCounter \
 *   --reducer=org.kiji.mapreduce.lib.reduce.LongSumReducer \
 *   --input="format=kiji table=${KIJI}/users" \
 *   --output="format=seq file=${HDFS_ROOT}/output.sequence_file nsplits=2" \
 *   --lib=target/kiji-music-0.1-SNAPSHOT-release/kiji-music-0.1-SNAPSHOT/lib/
 * </pre>
 */
package org.kiji.examples.music.gather;
