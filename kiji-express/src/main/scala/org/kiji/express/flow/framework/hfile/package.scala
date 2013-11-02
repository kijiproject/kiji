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

package org.kiji.express.flow.framework.hfile

/**
 * Provides the necessary implementation to allow Scalding jobs to write directly to HFiles. To
 * support this, simply subclass the HFileKijiJob. The only requirement is that you provide
 * a command line argument hFileOutput which references the location on HDFS where the final
 * HFiles are to be stored.
 *
 * Under the covers, Scalding will be configured to write to HFiles either by manipulating the
 * current Cascading flow if the defined flow has no reducers OR by redirecting output to a
 * temporary location where a secondary job will re-order the results into something that
 * HBase can load. The reason for this is because KeyValues must be sorted according to a
 * TotalOrderPartitioner that relies on the region splits of HBase so that KeyValues are placed
 * in the right region.
 */

