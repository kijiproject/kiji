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

package org.kiji.schema.shell.util

import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.schema.Kiji
import org.kiji.schema.KijiInstaller
import org.kiji.schema.KijiURI
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.shell.input.NullInputSource

import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.KijiSystem
import org.kiji.schema.shell.AbstractKijiSystem

trait KijiIntegrationTestHelpers {

  def getKijiSystem(): AbstractKijiSystem = {
    return new KijiSystem
  }

  private val mNextInstanceId = new AtomicInteger(0)

  /**
   * @return the name of a unique Kiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): KijiURI = {
    val id = mNextInstanceId.incrementAndGet()
    val uri = KijiURI.newBuilder().withZookeeperQuorum(Array(".fake." +
      id)).withInstanceName(getClass().getName().replace(".", "_")).build()
    installKiji(uri)
    return uri
  }

  /**
   * Install a Kiji instance.
   */
  def installKiji(instanceURI: KijiURI): Unit = {
    KijiInstaller.get().install(instanceURI, HBaseConfiguration.create())

    // This requires a system-2.0-based Kiji. Explicitly set it before we create
    // any tables, if it's currently on system-1.0.
    val kiji: Kiji = Kiji.Factory.open(instanceURI)
    try {
      val curDataVersion: ProtocolVersion = kiji.getSystemTable().getDataVersion()
      val system20: ProtocolVersion = ProtocolVersion.parse("system-2.0")
      if (curDataVersion.compareTo(system20) < 0) {
        kiji.getSystemTable().setDataVersion(system20)
      }
    } finally {
      kiji.release()
    }
  }

  def environment(uri: KijiURI, kijiSystem: AbstractKijiSystem): Environment = {
    new Environment(
      instanceURI=uri,
      printer=System.out,
      kijiSystem=kijiSystem,
      inputSource=new NullInputSource(),
      modules=List(),
      isInteractive=false)
  }
}
