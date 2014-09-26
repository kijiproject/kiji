package org.kiji.commons.scala

import com.google.common.base.Optional

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/** Implicit converters between Java and Scala idioms. */
@ApiAudience.Framework
@ApiStability.Experimental
object JavaConverters {

  /**
   * Implicit converter between scala Option and guava Optional.
   *
   * @param opt Option to convert to an Optional.
   * @tparam T Type of the value held in the Option.
   */
  implicit class OptionAsOptional[T](
      opt: Option[T]
  ) {
    def asJava: Optional[T] = {
      opt match {
        case Some(x) => {
          // Because Some may be explicitly created with a null value, we must manually check if the
          // value is null.
          if (x == null) {
            Optional.absent()
          } else {
            Optional.of(x)
          }
        }
        case None => Optional.absent()
      }
    }
  }

  /**
   * Implicit converter between guava Optional and scala Option.
   *
   * @param opt Optional to convert to an Option.
   * @tparam T Type of the value held in the Optional.
   */
  implicit class OptionalAsOption[T](
      opt: Optional[T]
  ) {
    def asScala: Option[T] = Option(opt.orNull())
  }
}
