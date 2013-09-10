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

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.ArrayList

import com.google.common.collect.Lists

import org.apache.avro.Schema

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.avro.CellSchema
import org.kiji.schema.avro.SchemaStorage
import org.kiji.schema.avro.SchemaType

import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.util.ForkJvm
import org.kiji.schema.shell.util.PrintSchemaForClass

/**
 * A schema specified by the classname of an Avro specific class.
 *
 * The class name is provided as a list of string "parts" that represent package
 * and class names; these should be concatenated with the string "." interleaved
 * between parts.
 */
@ApiAudience.Private
final class ClassSchemaSpec(private val parts: List[String]) extends SchemaSpec with ForkJvm {
  /** The fully-qualified class name that represents the schema. */
  val className = {
    val sb = new StringBuilder
    var first = true
    parts.foreach { part =>
      if (!first) {
        sb.append(".")
      }
      sb.append(part)
      first = false
    }
    sb.toString()
  }

  override def toNewCellSchema(cellSchemaContext: CellSchemaContext): CellSchema = {
    if (cellSchemaContext.supportsLayoutValidation()) {
      // layout-1.3 and up support layout validation.
      val avroValidationPolicy: AvroValidationPolicy =
          cellSchemaContext.getValidationPolicy().avroValidationPolicy

      val schema: Schema = getSchemaForClass(cellSchemaContext.env)

      // Use the schema table to find the actual uid associated with this schema.
      val uidForSchemaClass: Long = cellSchemaContext.env.kijiSystem.getOrCreateSchemaId(
          cellSchemaContext.env.instanceURI, schema)

      val avroSchema = AvroSchema.newBuilder().setUid(uidForSchemaClass).build()

      // Register the specified class as a valid reader and writer schema as well as the
      // default reader schema.
      val readers: ArrayList[AvroSchema] = Lists.newArrayList(avroSchema)
      val writers: ArrayList[AvroSchema] = Lists.newArrayList(avroSchema)

      // For now (layout-1.3), adding to the writers list => adding to the "written" list.
      val written: ArrayList[AvroSchema] = Lists.newArrayList(avroSchema)

      return CellSchema.newBuilder()
          .setStorage(SchemaStorage.UID)
          .setType(SchemaType.AVRO)
          .setValue(null)
          .setAvroValidationPolicy(avroValidationPolicy)
          .setSpecificReaderSchemaClass(className)
          .setDefaultReader(avroSchema)
          .setReaders(readers)
          .setWritten(written)
          .setWriters(writers)
          .build()
    } else {
      // Create a legacy (non-validating) class-based CellSchema.
      return CellSchema.newBuilder()
          .setType(SchemaType.CLASS)
          .setStorage(SchemaStorage.UID)
          .setValue(className)
          .build()
    }
  }

  override def addToCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {
    if (!cellSchemaContext.supportsLayoutValidation()) {
      // This method was called in an inappropriate context; the CellSchema
      // should be overwritten entirely in older layouts.
      throw new DDLException("SchemaSpec.addToCellSchema() can only be used on validating layouts")
    }

    val avroSchema: Schema = getSchemaForClass(cellSchemaContext.env)

    // Add to the main reader/writer/etc lists.
    addAvroToCellSchema(avroSchema, cellSchema, cellSchemaContext.schemaUsageFlags,
        cellSchemaContext.env)

    return cellSchema
  }

  override def dropFromCellSchema(cellSchema: CellSchema, cellSchemaContext: CellSchemaContext):
      CellSchema = {

    if (!cellSchemaContext.supportsLayoutValidation()) {
      // This method was called in an inappropriate context; the CellSchema
      // should be overwritten entirely in older layouts.
      throw new DDLException("SchemaSpec.dropFromCellSchema() can only be used "
          + "on validating layouts")
    }

    val avroSchema: Schema = getSchemaForClass(cellSchemaContext.env)

    // Drop from the main reader/writer/etc lists.
    dropAvroFromCellSchema(avroSchema, cellSchema, cellSchemaContext.schemaUsageFlags,
        cellSchemaContext.env)

    return cellSchema
  }


  override def toString(): String = { className }

  /**
   * Look up the schema JSON associated with the 'className' argument to this schema spec.
   *
   * The class may not be on the classpath of the current JVM, but the user may have defined
   * the location of the class with an ADD JAR command earlier. We fork a jvm with our classpath
   * plus any jars listed in ADD JAR commands, and use this to look up the associated
   * schema class through reflection.
   *
   * @param the environment in which we execute this operation.
   * @return an avro Schema object representing the class' schema.
   */
  private def getSchemaForClass(env: Environment): Schema = {
    // Fork a JVM to run a class that prints the schema for a class.
    val stdoutBytes: ByteArrayOutputStream = new ByteArrayOutputStream()
    val stdoutCapture: PrintStream = new PrintStream(stdoutBytes, false, "UTF-8")
    val ret: Int = forkJvm(env, classOf[PrintSchemaForClass].getName(), List(), List(className),
        stdoutCapture)

    if (0 != ret) {
      // Invalid error return from the subprocess.
      throw new DDLException("Could not look up schema for class " + className + "; ret=" + ret)
    }

    stdoutCapture.close()
    stdoutBytes.close()
    val schemaJson: String = stdoutBytes.toString("UTF-8")
    return new Schema.Parser().parse(schemaJson)
  }
}
