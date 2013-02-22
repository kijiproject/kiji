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
 * This package contains reducers used in the KijiMusic tutorial.
 *
 * Reducers can be run from the command line as part of a kiji mapreduce command:
 *
 * kiji mapreduce \
 * --mapper=org.kiji.examples.music.map.IdentityMapper \
 *  --reducer=org.kiji.examples.music.reduce.TopNextSongsReducer \
 *  --input="format=avrokv file=${HDFS_ROOT}/output.sequentialPlayCount" \
 *  --output="format=kiji table=${KIJI}/songs nsplits=1" \
 *  --lib=${LIBS_DIR}
 */
package org.kiji.examples.music.reduce;
