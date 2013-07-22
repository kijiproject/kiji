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

package org.kiji.express.tool

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream

import scala.io.Source

import com.google.common.io.Files
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.Tool
import com.twitter.util.Eval
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.ToolRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.express.util.Resources.doAndClose

/**
 * ScriptRunner provides the machinery necessary to be able to run uncompiled KijiExpress
 * scripts.
 *
 * An uncompiled KijiExpress should be written as if it were a script inside of a KijiJob
 * class. Scripts can access command line arguments by using the Scalding Args object with the name
 * args. Scripts compiled by ScriptRunner get wrapped in a KijiJob constructor:
 * {{{
 *   // Added by ScriptRunner:
 *   { args: com.twitter.scalding.Args =>
 *     new KijiJob(args) {
 *
 *       // User code here.
 *
 *     }
 *   }
 * }}}
 *
 * To run a script using ScriptRunner call the express jar command with the following syntax.
 * {{{
 *   express jar </path/to/express/jar> org.kiji.express.ScriptRunner \
 *       </path/to/script> [other options here]
 * }}}
 */
@deprecated
class ScriptRunner extends Tool {
  import ScriptRunner._

  /** Directory to place compiled artifacts in. */
  private[express] val tempDir: File = Files.createTempDir()

  /** Directory to place compiled classes in. Exists as a folder inside of 'tempDir'. */
  private[express] val compileFolder: File = new File("%s/classes".format(tempDir.getPath()))
  compileFolder.mkdir()

  /** Import statements to add to each script. */
  private[express] val imports: Seq[String] = Seq(
      "import com.twitter.scalding._",
      "import org.kiji.express._",
      "import org.kiji.express.DSL._")

  /** Code to insert before each script. */
  private[express] val before =
      "{ args: com.twitter.scalding.Args => new KijiJob(args) {"

  /** Code to insert after each script. */
  private[express] val after = "} }"

  /**
   * Builds a jar entry and writes it to the specified jar output stream.
   *
   * @param source directory or file to write to the specified JarOutputStream.
   * @param entryPath that the new jar entry will be added under.
   * @param target stream to write to.
   */
  private[express] def addToJar(source: File, entryPath: String, target: JarOutputStream) {
    source
        .listFiles
        .foreach {
          case file if file.isFile() && file.getName().endsWith(".class") => {
            val entry: JarEntry = new JarEntry(entryPath + file.getName())
            entry.setTime(file.lastModified())
            target.putNextEntry(entry)

            doAndClose(new BufferedInputStream(new FileInputStream(file))) { inputStream =>
              val buffer = new Array[Byte](inputStream.available())
              inputStream.read(buffer)
              target.write(buffer)
            }
            target.closeEntry()
          }
          case dir if dir.isDirectory() => {
            // Make sure the path for this directory ends with a '/'.
            val path: String = entryPath + dir.getName() + "/"

            // Add a new jar entry for the directory.
            val entry: JarEntry = new JarEntry(path)
            entry.setTime(dir.lastModified())
            target.putNextEntry(entry)
            target.closeEntry()

            // Recursively process nested files/directories.
            dir.listFiles().foreach { nested: File => addToJar(nested, path, target) }
          }
          case other => logger.debug("ignoring %s".format(other.getPath()))
        }
  }

  /**
   * Builds a jar containing the .class files in the specified folder and all subfolders.
   *
   * @param source directory to include in jar.
   * @param target path to desired jar.
   */
  private[express] def buildJar(source: File, target: File) {
    require(source.isDirectory(), "Source file %s is not a directory".format(source.getPath()))

    doAndClose(new JarOutputStream(new FileOutputStream(target))) { jarStream =>
      addToJar(source, "", jarStream)
    }
  }

  /**
   * Compiles a script to the specified folder. Before compilation occurs, the code contained within
   * the specified script will be wrapped in a scaling job constructor. After compilation, this job
   * constructor, in the form of a function, will be returned.
   *
   * @param script to compile.
   * @param outputDirectory that compiled classes will be placed in.
   * @return a function that will construct a KijiExpress job given command line arguments.
   */
  private[express] def compileScript(script: String, outputDirectory: File): (Args) => KijiJob = {
    // Create an evaluator that will compile the script to a temporary directory.
    logger.info("compiling classes to %s".format(outputDirectory))
    val compiler = new Eval(Some(outputDirectory))

    // Modify the script so that it returns a job constructor.
    val preparedScript: String = {
      val importBlock = imports.reduce { _ + "\n" + _ }
      "%s\n%s\n%s\n%s".format(importBlock, before, script, after)
    }

    // Check and compile the code.
    compiler.check(preparedScript)
    compiler.apply(preparedScript)
  }

  /**
   * Allows you to set the job for the Tool to run. This method will always
   * throw an exception.
   *
   * @param jobc is the constructor of the job to run.
   */
  override def setJobConstructor(jobc: (Args) => Job) {
    sys.error("ScriptRunner only runs jobs that haven't been compiled yet.")
  }

  override protected def getJob(args: Args): Job = {
    val scriptFile: File = {
      require(!args.positional.isEmpty, "Usage: ScriptRunner <scriptfile> --local|--hdfs [args...]")
      val List(script, _*) = args.positional
      new File(script)
    }
    require(scriptFile.exists(), "%s does not exist".format(scriptFile.getPath()))
    require(scriptFile.isFile(), "%s is not a file".format(scriptFile.getPath()))

    // Compile the script.
    val script: String = doAndClose(Source.fromFile(scriptFile)) { source: Source =>
      source.mkString
    }
    val jobc: (Args) => KijiJob = compileScript(script, compileFolder)

    // Build a jar.
    val compileJar: File = new File("%s/%s.jar".format(tempDir.getPath(), scriptFile.getName()))
    logger.info("building %s".format(compileJar))
    buildJar(compileFolder, compileJar)
    assume(
        compileJar.isFile(),
        "%s is not a file.".format(compileJar.getPath()))
    assume(
        compileJar.getPath().endsWith(".jar"),
        "%s should end with '.jar'.".format(compileJar.getPath()))
    assume(
        compileJar.exists(),
        "%s does not exist.".format(compileJar.getPath()))

    // Get this tool's configuration and store the compiled jar's location in 'tmpjars'.
    val tmpjars = Option(getConf().get("tmpjars"))
        .map { "," + _ }
        .getOrElse("")
    getConf().set("tmpjars", "file://" + compileJar.getPath() + tmpjars)

    jobc(args + ("" -> args.positional.tail))
  }
}

/**
 * The companion object for ScriptRunner, which compiles and runs KijiExpress scripts. This contains
 * the main method, which is the program entry point.
 */
object ScriptRunner {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ScriptRunner])

  /**
   * Compiles and runs the provided KijiExpress script.
   *
   * @param args passed in from the command line. The first argument to ScriptRunner is expected to
   *     be a path to the script to run. Any remaining flags get passed to the script itself.
   */
  def main(args: Array[String]) {
    logger.warn("Running uncompiled scripts is deprecated and will be removed in a future release.")
    ToolRunner.run(HBaseConfiguration.create(), new ScriptRunner, args)
  }
}
