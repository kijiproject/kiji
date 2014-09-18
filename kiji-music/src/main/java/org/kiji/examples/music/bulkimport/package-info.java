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
 * TODO: Update this with documentation on how to do bulk imports, once kiji-schema
 * supports bulk-import
 *
 * For the time being, bulk imports are run from the CLI:
 *
 * java -cp $KIJI_CLASSPATH:target/kiji-music-0.1.jar org.kiji.mapreduce.tools.KijiBulkImport \
 *   --input=text:kiji-music-users.txt \
 *   --table=users \
 *   --importer=org.kiji.examples.music.bulkimport.TrackPlaysBulkImporter \
 *   --output=kiji
 */
package org.kiji.examples.music.bulkimport;
