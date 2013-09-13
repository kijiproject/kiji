package org.kiji.schema.shell.util

import java.util.UUID

import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.schema.KijiInstaller
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.DDLParser
import org.kiji.schema.shell.KijiSystem
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.input.NullInputSource

trait KijiTestHelpers {
  /**
   * @return the name of a unique Kiji instance (that doesn't yet exist).
   */
  def getNewInstanceURI(): KijiURI = {
    val instanceName = UUID.randomUUID().toString().replaceAll("-", "_");
    return KijiURI.newBuilder("kiji://.env/" + instanceName).build()
  }

  /**
   * Install a Kiji instance.
   */
  def installKiji(instanceURI: KijiURI): Unit = {
    KijiInstaller.get().install(instanceURI, HBaseConfiguration.create())
  }

  /**
   * Get a new parser that's primed with the specified environment.
   */
  def getParser(environment: Environment): DDLParser = {
    new DDLParser(environment)
  }
}
