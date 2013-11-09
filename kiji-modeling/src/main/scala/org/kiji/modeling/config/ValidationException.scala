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

package org.kiji.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
/**
 * This exception is thrown when an error is encountered during validation of a JSON specification.
 *
 * @param msg explaining why the validation error occurred.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
class ValidationException(
    msg: String)
    extends RuntimeException(msg)

object ValidationException {

  private[kiji] def messageWithCauses(msg: String, causes: Seq[ValidationException]): String = {
    val causesMsg = causes
        .tail
        .foldLeft(causes.head.getMessage) { _ + "\n" + _.getMessage }

    if (msg == "") {
      causesMsg
    } else {
      msg + "\n" + causesMsg
    }
  }
}

/**
 * This exception is thrown when a ModelDefinition does not have valid fields specified.
 *
 * @param causes for this validation exception.
 * @param msg describing this exception. This can be left blank if desired.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
class ModelDefinitionValidationException(
    causes: Seq[ValidationException],
    msg: String = "")
    extends ValidationException(ValidationException.messageWithCauses(msg, causes))

/**
 * This exception is thrown when a ModelEnvironment does not have valid fields specified.
 *
 * @param causes for this validation exception.
 * @param msg describing this exception. This can be left blank if desired.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Sealed
class ModelEnvironmentValidationException(
    causes: Seq[ValidationException],
    msg: String = "")
    extends ValidationException(ValidationException.messageWithCauses(msg, causes))
