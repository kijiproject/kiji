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

package org.kiji.schema.shell.modules

import org.kiji.annotations.ApiStability
import org.kiji.annotations.ApiAudience
import org.kiji.schema.security.{KijiUser, KijiPermissions}
import org.kiji.schema.shell.spi.ParserPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory
import org.kiji.schema.shell.spi.HelpPlugin
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.DDLParserHelpers
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand
import org.kiji.schema.{Kiji, KConstants, KijiURI}

/**
 * A plugin for commands for Kiji access permissions.
 *
 * Adds commands for granting and revoking access to users on Kiji instaces.
 */
@ApiAudience.Private
@ApiStability.Experimental
class SecurityParserPluginFactory  extends ParserPluginFactory with HelpPlugin {
  override def getName(): String = "security"

  override def helpText(): String = {
    return """ Adds commands to interact with permissions in Kiji from the Kiji shell.
             |   Parser plugin that matches the syntax:
             |
             |   GRANT { READ | WRITE | GRANT } [PRIVILEGES]
             |        ON INSTANCE 'kiji://instance_uri'
             |        TO [ USER ] user_name;
             |
             |    REVOKE { READ | WRITE | GRANT } [PRIVILEGES]
             |        ON INSTANCE 'kiji://instance_uri'
             |        FROM [ USER ] 'user_name';
             |""".stripMargin
  }

  override def create(env: Environment): ParserPlugin = {
    return new SecurityParserPlugin(env);
  }

  /**
   * Represents a change in permissions that should be executed by Kiji.
   */
  sealed trait PermissionsChange

  /**
   * Represents a grant command that should be executed by Kiji.
   *
   * @param actions to grant.
   * @param instance to grant them on.
   * @param user to whom to grant the permissions for actions on instance.
   */
  case class GrantCommand(
      actions: List[KijiPermissions.Action],
      instance: KijiURI,
      user: KijiUser)
      extends PermissionsChange

  /**
   * Represents a revoke command that should be executed by Kiji.
   *
   * @param actions to revoke.
   * @param instance to revoke them on.
   * @param user from whom to revoke the permissions for actions on instance.
   */
  case class RevokeCommand(
      actions: List[KijiPermissions.Action],
      instance: KijiURI,
      user: KijiUser)
      extends PermissionsChange

  /**
   * Parser plugin that matches the syntax:
   *
   * GRANT { READ | WRITE | GRANT } [PRIVILEGES]
   *     ON INSTANCE 'kiji://instance_uri'
   *     TO [ USER ] user_name
   *
   * REVOKE { READ | WRITE | GRANT } [PRIVILEGES]
   *     ON INSTANCE 'kiji://instance_uri'
   *     FROM [ USER ] 'user_name'
   */
  final class SecurityParserPlugin(val env: Environment)
      extends ParserPlugin
      with DDLParserHelpers {

    // Specifies what instance access is being changed on.
    def instanceSpec: Parser[KijiURI] = (
      i("ON") ~> i("INSTANCE") ~> singleQuotedString ^^ { case uri =>
        KijiURI.newBuilder(uri).build() }
      )

    // Specifies which user's access is being changed in a GRANT.
    def userGrantSpec: Parser[KijiUser] = (
      i("TO") ~> opt(i("USER")) ~> optionallyQuotedString ^^
          { case username => KijiUser.fromName(username) }
      )

    // Specifies which user's access is being changed in a REVOKE.
    def userRevokeSpec: Parser[KijiUser] = (
      i("FROM") ~> opt(i("USER")) ~> optionallyQuotedString ^^
        { case username => KijiUser.fromName(username) }
      )

    // Full GRANT command.
    def grant: Parser[GrantCommand] = (
      i("GRANT") ~> rep(action) ~ opt(i("PRIVILEGES")) ~ instanceSpec ~ userGrantSpec ^^
        { case actions ~ _ ~ instance ~ user => new GrantCommand(actions, instance, user)}
      )

    // Full REVOKE command.
    def revoke: Parser[RevokeCommand] = (
      i("REVOKE") ~> rep(action) ~ opt(i("PRIVILEGES")) ~ instanceSpec ~ userRevokeSpec ^^
        { case actions ~ _ ~ instance ~ user => new RevokeCommand(actions, instance, user)}
      )

    def action: Parser[KijiPermissions.Action] = (
        i("READ") ^^ (_ => KijiPermissions.Action.READ)
      | i("WRITE") ^^ (_ => KijiPermissions.Action.WRITE)
      | i("GRANT") ^^ (_ => KijiPermissions.Action.GRANT))

    override def command(): Parser[DDLCommand] = ( (grant | revoke)  <~ ";"
        ^^ { case permissionsChange => new UserSecurityCommand(env, permissionsChange) }
      )
  }

  final class UserSecurityCommand(
      val env: Environment,
      val permissionsChange: PermissionsChange)
      extends DDLCommand {

    override def exec(): Environment = {
      permissionsChange match {
        case GrantCommand(actions, instance, user) => {
          actions.map { case action: KijiPermissions.Action =>
            env.kijiSystem.getSecurityManager(instance).grant(user, action)
          }
        }
        case RevokeCommand(actions, instance, user) => {
          actions.map { case action: KijiPermissions.Action =>
            env.kijiSystem.getSecurityManager(instance).revoke(user, action)
          }
        }
      }
      return env
    }
  }
}
