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

package org.kiji.express.flow

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import com.fasterxml.jackson.databind.JsonSerializable
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.twitter.scalding.Args
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.GeneratedTupleAdders
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Mappable
import com.twitter.scalding.Mode
import com.twitter.scalding.RichPipe
import com.twitter.scalding.Source
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import com.twitter.scalding.TypedPipe
import com.twitter.scalding.TypedSource
import com.twitter.scalding.typed.PipeTExtensions

import org.kiji.express.flow.util.AvroTupleConversions
import org.kiji.express.flow.util.PipeConversions

/**
 * A container to allow scalding code to be executed outside of a scalding Job object. The
 * constructing objects of this class and its subclasses should be treated as nonserializable.
 * Care should be given to avoid letting members of this class into closures, and nonserializable
 * members should be flagged as @transient. The config object is not java serializable and should
 * not be used on the cluster side code. The best practice for parsing the config object is to
 * parse it in function scope and not in member scope or closures.
 *
 * @param config A Json configuration used by the container.
 * @param mode An implicit Mode object for execution of scalding commands.
 * @param flowDef An implicit FlowDef object for execution of scalding commands.
 */
abstract class ExpressContainer(
    @transient // config is not serializable and should never used on the cluster machine.
    final val config: ObjectNode,
    implicit val mode: Mode,
    @transient // flowDef is not serializable and is never used on the cluster machine.
    implicit val flowDef: FlowDef
) extends PipeConversions
    with AvroTupleConversions
    with FieldConversions
    with GeneratedTupleAdders
    with Serializable {

  // Implicits taken from com.twitter.scalding.Job so that programming with this object should be
  // the same as programming in a KijiJob.

  /**
   * "you should never call this directly, it is here to make
   * the DSL work.  Just know, you can treat a Pipe as a RichPipe
   * within a Job"
   */
  implicit def pipeToRichPipe(pipe : Pipe): RichPipe = new RichPipe(pipe)
  /**
   * "This implicit is to enable RichPipe methods directly on Source
   * objects, such as map/flatMap, etc...
   *
   * Note that Mappable is a subclass of Source, and Mappable already
   * has mapTo and flatMapTo BUT WITHOUT incoming fields used (see
   * the Mappable trait). This creates some confusion when using these methods
   * (this is an unfortuate mistake in our design that was not noticed until later).
   * To remove ambiguity, explicitly call .read on any Source that you begin
   * operating with a mapTo/flatMapTo."
   */
  implicit def sourceToRichPipe(src : Source): RichPipe = new RichPipe(src.read)
  /**
   * "This converts an Iterable into a Pipe or RichPipe with index (int-based) fields"
   */
  implicit def toPipe[T](iter : Iterable[T])
        (implicit set: TupleSetter[T], conv : TupleConverter[T]): Pipe =
      IterableSource[T](iter)(set, conv).read

  implicit def iterableToRichPipe[T](iter : Iterable[T])
        (implicit set: TupleSetter[T], conv : TupleConverter[T]): RichPipe =
      RichPipe(toPipe(iter)(set, conv))
  /**
   * "This is implicit so that a Source can be used as the argument
   * to a join or other method that accepts Pipe."
   */
  implicit def read(src : Source) : Pipe = src.read

  implicit def pipeTExtensions(pipe : Pipe) : PipeTExtensions = new PipeTExtensions(pipe)

  implicit def mappableToTypedPipe[T](src: Mappable[T])
        (implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    TypedPipe.from(src)(flowDef, mode)

  implicit def sourceToTypedPipe[T](src: TypedSource[T])
        (implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    TypedPipe.from(src)(flowDef, mode)
}

object ExpressContainer {
  /**
   * Construct a new ExpressContainer object with a given configuration. The resulting container
   * object must have a constructor of type (ObjectNode, Mode, FlowDef).
   *
   * @param config a json configuration object. The field 'class' must be filled with a fully
   *     qualified class name of the Container to be constructed.
   * @param mode an implicit Mode object passed in from the KijiJob.
   * @param flowDef an implicit FlowDef object passed in from the KijiJob.
   * @return an Container object constructed from the json configuration.
   */
  def apply(config: String)(implicit mode: Mode, flowDef: FlowDef): ExpressContainer = {
    val configObject: ObjectNode = new ObjectMapper().readTree(config).asInstanceOf[ObjectNode]
    apply(configObject)
  }

  /**
   * Construct a new ExpressContainer object with a given configuration. The resulting container
   * object must have a constructor of type (ObjectNode, Mode, FlowDef).
   *
   * @param config a json configuration object. The field 'class' must be filled with a fully
   *     qualified class name of the Container to be constructed.
   * @param mode an implicit Mode object passed in from the KijiJob.
   * @param flowDef an implicit FlowDef object passed in from the KijiJob.
   * @return an Container object constructed from the json configuration.
   */
  def apply(config: ObjectNode)(implicit mode: Mode, flowDef: FlowDef): ExpressContainer = {
    val className = config.findValue("class").asText()
    Class.forName(className)
        .asSubclass(classOf[ExpressContainer])
        .getConstructor(classOf[ObjectNode], classOf[Mode], classOf[FlowDef])
        .newInstance(config, mode, flowDef)
        .asInstanceOf[ExpressContainer]
  }
}

/**
 * A factory to create ExpressContainer objects that implement a given trait.
 *
 * @tparam T The trait of the constructed containers. If the container does not implement this
 *     trait, the construction of the container will fail with a ClassCastException.
 */
trait ExpressContainerFactory[T] {
  /**
   * A factory method that constructs an ExpressContainer given a Key. This can only be called from
   * within a KijiJob. This will throw an exception if the resulting container does not implement
   * the interface T.
   *
   * @param key the key in the Args object that points to the configuration of the container.
   * @param kijiArgs Implicit Args object passed from a KijiJob.
   * @param mode Implicit Mode object passed from a KijiJob.
   * @param flowDef Implicit FlowDef object passed from a KijiJob.
   * @param interface The manifest of trait T. This is used to get the runtime class of the trait
   *     for exception handling.
   * @throws ClassCastException if the container does not implement interface T.
   * @return An express container cast to interface T.
   */
  def apply(
      key: String
  )(implicit
      kijiArgs: Args,
      mode: Mode,
      flowDef: FlowDef,
      interface: Manifest[T]
  ): T = {
    val container: ExpressContainer = ExpressContainer(kijiArgs(key))
    container match {
      case validContainer: T => validContainer
      case _ => throw new ClassCastException(
        s"Class ${container.getClass} does not implement interface ${interface.runtimeClass}"
      )
    }
  }
}
