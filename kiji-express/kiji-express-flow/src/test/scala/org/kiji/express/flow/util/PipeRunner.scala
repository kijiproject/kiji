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

package org.kiji.express.flow.util

import java.lang.reflect.Constructor

import scala.collection.mutable

import cascading.flow.Flow
import cascading.pipe.Pipe
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.JobTest
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Tsv
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import com.twitter.scalding.Write

/**
 * Module for providing a test framework around testing Express pipes.  Allows testing of pipes
 * independent of the Source and Sink.  See `runPipe`.
 */
object PipeRunner {

  /**
   * Unpacks [[scala.Product]] instances into [[cascading.tuple.Tuple]] instances.
   * @param n number of fields expected in product type
   */
  private class ProductTupleSetter[T](n: Int) extends TupleSetter[T] {
    def arity: Int = n

    def apply(t: T): Tuple = {
      if (n == 1) {
        new Tuple(t.asInstanceOf[Object])
      } else {
        val p: Product = t.asInstanceOf[Product]
        require(p.productArity == n)
        val tuple = new Tuple()
        p.productIterator.foreach(tuple.add)
        tuple
      }
    }
  }

  /**
   * Converts [[cascading.tuple.TupleEntry]]s into scala Tuples via frightening black majik.
   */
  private class ProductTupleConverter[O] extends TupleConverter[O] {

    override def arity: Int = -1

    var ctors: Map[Int, Constructor[_]] = Map()
    private def tupleCtor(arity: Int) = {
      if (!ctors.contains(arity)) {
        ctors += arity -> Class.forName("scala.Tuple" + arity).getConstructors.apply(0)
      }
      ctors(arity)
    }

    override def apply(entry: TupleEntry): O = {
      val arity = entry.size()
      if (arity == 1) {
        entry.getObject(0).asInstanceOf[O]
      } else {
        tupleCtor(entry.size())
            .newInstance(Range(0, entry.size()).map(entry.getObject):_*)
            .asInstanceOf[O]
      }
    }
  }

  /**
   * Simulates running a pipe with given inputs containing given fields.  Returns a
   * [[scala.collection.immutable.List]] of result values.  The returned value type is not known
   * statically; therefore runtime type errors on the result are possible.
   *
   * @tparam I Type of input values.
   * @tparam O Type of output values.  Statically resolves to [[scala.Nothing]].
   * @param pipe Pipe to run.
   * @param fields Field names of input tuples.
   * @param input Iterable of input tuples.
   * @return List of output values.
   */
  def runPipe[I, O](pipe: Pipe, fields: Fields, input: Iterable[I]): List[O] = {
    val source = Tsv("input", fields)
    val sink = Tsv("output")

    implicit val setter: TupleSetter[I] = new ProductTupleSetter(fields.size())
    implicit val converter: TupleConverter[O] = new ProductTupleConverter[O]

    class InnerJob(args: Args) extends Job(args) {
      override def buildFlow(implicit mode : Mode): Flow[_]  = {
        validateSources(mode)
        mode.newFlowConnector(config).connect(
          pipe.getName, source.createTap(Read), sink.createTap(Write), pipe)
      }
    }

    var buffer: mutable.Buffer[O] = null
    val jobTest = JobTest(new InnerJob(_))
        .source(source, input)
        .sink(sink) { b: mutable.Buffer[O] => buffer = b}

    jobTest.run
    buffer.toList
  }
}
