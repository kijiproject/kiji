package com.wibidata.lang

class KijiSource(
    val dataRequest: KijiDataRequest,
    val tableURI: KijiURI) extends Source {

  override val hdfsScheme = new KijiScheme(dataRequest)

  override val createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val kijiScheme = hdfsScheme.asInstanceOf[KijiScheme]
    val tap = mode match {
      case Hdfs(_, _) => readOrWrite match {
        case Read => new KijiTap(tableURI, kijiScheme)
        case Write => error("Writing isn't supported currently.")
      }
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }
}
