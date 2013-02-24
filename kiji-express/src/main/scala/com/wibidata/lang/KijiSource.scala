package com.wibidata.lang

import cascading.tap.Tap
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Write

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI

case class KijiSource(
    dataRequest: KijiDataRequest,
    tableURI: KijiURI) extends Source {

  // override val hdfsScheme: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _] =
  override def hdfsScheme = new KijiScheme(dataRequest)

  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val kijiScheme = hdfsScheme.asInstanceOf[KijiScheme]
    val tap: Tap[_, _, _] = mode match {
      case Hdfs(_, _) => readOrWrite match {
        case Read => new KijiTap(tableURI, kijiScheme)
        case Write => sys.error("Writing isn't supported currently.")
      }
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }
}
