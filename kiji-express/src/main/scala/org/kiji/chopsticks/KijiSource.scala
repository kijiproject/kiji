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

package org.kiji.chopsticks

import cascading.scheme.Scheme
import cascading.tap.Tap
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.Source
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.OutputCollector

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI
import org.kiji.lang.KijiScheme
import org.kiji.lang.KijiTap

case class KijiSource(
    dataRequest: KijiDataRequest,
    tableURI: KijiURI,
    columnsToWrite: java.util.Map[String, String] = null) extends Source {

  // TODO: The arguments to the constructor shouldn't be so weirdly separated between
  // dataRequest and columnsToWrite. related to CHOP-24.
  override def hdfsScheme: Scheme[JobConf, RecordReader[_,_], OutputCollector[_,_], _, _] = {
    new KijiScheme(dataRequest, columnsToWrite)
        .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
  }

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val kijiScheme = hdfsScheme.asInstanceOf[KijiScheme]
    val tap: Tap[_, _, _] = mode match {
      case Hdfs(_,_) => new KijiTap(tableURI, kijiScheme).asInstanceOf[Tap[_, _, _]]

      // TODO(CHOP-30): Add a case for test modes. The KijiSource should ideally read from
      //     a fake kiji instance.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }
}
