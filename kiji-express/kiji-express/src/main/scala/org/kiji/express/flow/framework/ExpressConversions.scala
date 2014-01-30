package org.kiji.express.flow.framework

import cascading.pipe.Pipe
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.RichPipe
import com.twitter.scalding.TupleConversions

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.util.AvroTupleConversions

/**
 * Object containing various implicit conversions supporting the Scalding DSL specific to
 * KijiExpress. Most of these conversions come from Scalding's Job class. Does not include implicit
 * conversions that require an implicit FlowDef object (source -> pipe/richPipe)
 */
@ApiAudience.Framework
@ApiStability.Stable
trait ExpressConversions
    extends TupleConversions
    with FieldConversions
    with AvroTupleConversions {

  /**
   * Converts a Cascading Pipe to a Scalding RichPipe. This method permits implicit conversions from
   * Pipe to RichPipe.
   *
   * @param pipe to convert to a RichPipe.
   * @return a RichPipe wrapping the specified Pipe.
   */
  implicit def pipeToRichPipe(pipe: Pipe): RichPipe = new RichPipe(pipe)

  /**
   * Converts a Scalding RichPipe to a Cascading Pipe. This method permits implicit conversions from
   * RichPipe to Pipe.
   *
   * @param richPipe to convert to a Pipe.
   * @return the Pipe wrapped by the specified RichPipe.
   */
  implicit def richPipeToPipe(richPipe: RichPipe): Pipe = richPipe.pipe

}
