package org.kiji.commons.scala

import com.google.common.base.Optional
import org.junit.Assert
import org.junit.Test

class TestJavaConverters {

  private def testScalaToJava[T](
      option: Option[T]
  ): Unit = {
    import org.kiji.commons.scala.JavaConverters.OptionAsOptional
    val optional: Optional[T] = option.asJava
    option match {
      case Some(x) => Assert.assertEquals(x, optional.get)
      case None => Assert.assertFalse(optional.isPresent)
    }
  }

  private def testJavaToScala[T](
      optional: Optional[T]
  ): Unit = {
    import org.kiji.commons.scala.JavaConverters.OptionalAsOption
    val option: Option[T] = optional.asScala
    option match {
      case Some(x) => Assert.assertEquals(optional.get, x)
      case None => Assert.assertFalse(optional.isPresent)
    }
  }

  @Test
  def converts(): Unit = {
    testScalaToJava(Some("abc"))
    testScalaToJava(Some(1))
    testScalaToJava(None)

    testJavaToScala(Optional.of("abc"))
    testJavaToScala(Optional.of(1))
    testJavaToScala(Optional.absent())
  }

  @Test
  def convertsSomeOfNull(): Unit = {
    import JavaConverters.OptionAsOptional
    val option: Option[String] = Some(null)
    val optional: Optional[String] = option.asJava
    Assert.assertFalse(optional.isPresent)
  }
}
