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

package org.kiji.schema.shell.ddl

import java.io.File

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.LocalJarFile
import com.google.common.io.Files

class TestUseJarCommand extends CommandTestCase {
  "UseJarCommand" should {
    "should validate the jar file exists" in {
      val command = new UseJarCommand(env, LocalJarFile("/does/not/exist.jar"))
      command.validateJar() must throwAn[DDLException](
          "The path you specified for the jar does not exist.")
    }

    "should validate the jar file is not a directory" in {
      val tempDir = Files.createTempDir()
      val jarLocation = LocalJarFile(tempDir.getAbsolutePath())
      val command = new UseJarCommand(env, jarLocation)
      command.validateJar() must throwA[DDLException](
          "The path you specified for the jar points to a directory.")
    }

    "should validate the jar file ends in .jar" in {
      val tempDir = Files.createTempDir()
      val jarFile = new File(tempDir, "myFile.txt")
      jarFile.createNewFile()
      val jarLocation = LocalJarFile(jarFile.getAbsolutePath())
      val command = new UseJarCommand(env, jarLocation)
      command.validateJar() must throwA[DDLException](
          "You must specify the path to a file with extension .jar or .JAR")
    }

    "should add a jar file to the environment" in {
      val tempDir = Files.createTempDir()
      val jarFile = new File(tempDir, "myJar.jar")
      jarFile.createNewFile()
      val jarLocation = LocalJarFile(jarFile.getAbsolutePath())
      val command = new UseJarCommand(env, jarLocation)
      val envWithJar = command.exec()
      envWithJar.libJars.size must beEqualTo(1)
      val LocalJarFile(actualPath) = envWithJar.libJars(0)
      jarFile.getAbsolutePath() must beEqualTo(actualPath)
    }
  }
}
