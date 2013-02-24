package com.wibidata.lang

import scala.io.Source
import java.io.File

import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.util.ResourceUtils

object NewsgroupLoader {
  /**
   * Imports the newsgroup example into a kiji postings table.
   *
   * Usage:
   *   kiji jar <path/to/this/jar> com.wibidata.lang.NewsgroupLoader <kiji://uri.to.kiji.instance> <path/to/newsgroups/root/>
   *
   * Tables:
   *   words
   *     CREATE TABLE words WITH DESCRIPTION 'Words in the 20Newsgroups dataset.'
   *     ROW KEY FORMAT HASHED
   *     WITH LOCALITY GROUP default WITH DESCRIPTION 'Main storage.' (
   *       MAXVERSIONS = 1,
   *       TTL = FOREVER,
   *       COMPRESSED WITH GZIP,
   *       FAMILY info WITH DESCRIPTION 'Basic information' (
   *         word "string" WITH DESCRIPTION 'The word.'
   *       )
   *     )
   */
  def main(args: Array[String]) {
    // Read in command line arguments.
    val uri = KijiURI.newBuilder(args(0)).build()
    val root = new File(args(1))

    if (!root.isDirectory()) { sys.error("Newsgroup root must be a folder (was: %s)".format(root.getPath())) }

    // Open connections to kiji.
    val kiji = Kiji.Factory.open(uri)
    val table = kiji.openTable("words")
    val writer = table.openTableWriter()

    // Parse the postings.
    type Post = String
    type Newsgroup = Seq[Post]
    def getWords(posting: File): Seq[String] = {
      println("Reading file '%s'".format(posting.getPath()))
      try {
        return Source
            .fromFile(posting.getPath())
            .mkString
            .split("""\s+""")
      } catch {
        case _ => {
          println("Ignored file '%s'".format(posting.getPath()))
          return Seq()
        }
      }
    }
    for {
      newsgroup <- root.listFiles()
      posting <- newsgroup.listFiles()
      word <- getWords(posting)
    } writer.put(table.getEntityId(word), "info", "word", word)

    // Cleanup connections.
    ResourceUtils.releaseOrLog(kiji)
    ResourceUtils.closeOrLog(table)
    ResourceUtils.closeOrLog(writer)
  }
}
