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

package org.kiji.express.flow.tool

import java.io.File

import com.google.common.io.Files
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TmpJarsToolSuite extends FunSuite {

  test("entryToFile transforms string paths to files.") {
    assert(new File("/is/a/classpath/entry") == TmpJarsTool.entryToFile("/is/a/classpath/entry"))
  }

  def makeGlobTestDirectory(): File = {
    // Make a temporary directory and put a directory, a .jar file, and a .txt file in it.
    val tempDirectory = Files.createTempDir()
    val subDirectory = new File(tempDirectory, "subdir")
    subDirectory.mkdir()
    val jarFile = new File(tempDirectory, "aJar.jar")
    jarFile.createNewFile()
    val txtFile = new File(tempDirectory, "aText.txt")
    txtFile.createNewFile()
    tempDirectory
  }

  test("globToFiles will unglob a file that is a glob (whose name is *).") {
    val globTestDir = makeGlobTestDirectory()
    val globTest = new File(globTestDir, "*")
    val unglobbedFiles = TmpJarsTool.globToFiles(globTest)
    assert(2 == unglobbedFiles.size)

    assert(unglobbedFiles.contains(new File(globTestDir, "aJar.jar")))
    assert(unglobbedFiles.contains(new File(globTestDir, "aText.txt")))
    assert(!unglobbedFiles.contains(new File(globTestDir, "subdir")))
  }

  test("globToFiles will leave a file whose name is not * alone.") {
    val testDir = new File("/i/am/a/file")
    val unGlobbedFiles = TmpJarsTool.globToFiles(testDir)
    assert(1 == unGlobbedFiles.size)
    assert(testDir == unGlobbedFiles(0))
  }

  test("isJar says yes to a .jar file.") {
    assert(TmpJarsTool.isJar(new File("/i/am/a/jarFile.jar")))
  }

  test("isJar says no to a file that isn't a .jar file.") {
    assert(!TmpJarsTool.isJar(new File("/i/am/not/a/jarFile.txt")))
  }

  test("getJarsFromClasspath will ignore empty classpath entries.") {
    val classpath = ":/i/am/aJar.jar::/i/am/anotherJar.jar:"
    val jars = TmpJarsTool.getJarsFromClasspath(classpath)
    assert(2 == jars.size)
    assert(jars.contains(new File("/i/am/aJar.jar")))
    assert(jars.contains(new File("/i/am/anotherJar.jar")))
  }

  test("getJarsFromClasspath will ignore directories.") {
    val globTestDir = makeGlobTestDirectory()
    val classpath = globTestDir.getAbsolutePath + ":" + new File("/i/am/aJar.jar")
    val jars = TmpJarsTool.getJarsFromClasspath(classpath)
    assert(1 == jars.size)
    assert(jars.contains(new File("/i/am/aJar.jar")))
  }

  test("getJarsFromClasspath will unglob glob entries and leave regular files alone.") {
    val globTestDir = makeGlobTestDirectory()
    val globTest = new File(globTestDir, "*")
    val classpath = globTest.getAbsolutePath + ":" + "/i/am/aJar.jar"
    val jars = TmpJarsTool.getJarsFromClasspath(classpath)
    assert(2 == jars.size)
    assert(jars.contains(new File(globTestDir, "aJar.jar")))
    assert(jars.contains(new File("/i/am/aJar.jar")))
  }

  test("getJarsFromClasspath will ignore unJars.") {
    val classpath = "/i/am/aJar.jar:/i/am/text.txt"
    val jars = TmpJarsTool.getJarsFromClasspath(classpath)
    assert(1 == jars.size)
    assert(jars.contains(new File("/i/am/aJar.jar")))
  }

  test("getjarsFromClasspath will transform a classpath into jar files accessible from that "
      + "classpath.") {
    val globTestDir = makeGlobTestDirectory()
    val globTest = new File(globTestDir, "*")
    val anotherTmpDir = Files.createTempDir()
    val classpath = globTest.getAbsolutePath +
        ":" + "/i/am/aJar.jar" + ":" + "/i/am/text.txt" + ":" + anotherTmpDir.getAbsolutePath
    val jars = TmpJarsTool.getJarsFromClasspath(classpath)
    assert(2 == jars.size)
    assert(jars.contains(new File(globTestDir, "aJar.jar")))
    assert(jars.contains(new File("/i/am/aJar.jar")))
  }
}
