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

package org.kiji.schema.shell

import java.io.IOException
import jline.UnixTerminal
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KijiInvalidNameException
import org.kiji.schema.shell.ddl.DDLCommand
import org.kiji.schema.shell.ddl.ErrorCommand
import org.kiji.schema.shell.spi.HelpPlugin
import org.kiji.schema.shell.spi.ParserPluginFactory
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.util.Bytes

/**
 * An object that processes user input.
 *
 * If throwOnSyntaxErr is true, then the processUserInput() method will
 * throw DDLExceptions in addition to printing to stdout.
 */
@ApiAudience.Framework
@ApiStability.Evolving
final class InputProcessor(val throwOnErr: Boolean = false) {
  private final val Log = LoggerFactory.getLogger(classOf[InputProcessor])

  val PROMPT_STR = "schema> "
  val CONTINUE_PROMPT_STR = "     -> "

  /**
   * Print help message to the output console of the environment.
   *
   * @param env the environment that contains the output console.
   */
  private def printHelp(env: Environment): Unit = {
    val helpLines = """
        |    KijiSchema DDL Shell
        |
        |Allows you to run layout definition language commands to create, modify,
        |inspect, and delete Kiji table layouts.
        |
        |Commands in this terminal operate on the metadata maintained by KijiSchema;
        |they do not operate on the data stored in HBase tables themselves.
        |
        |There are two special commands that may be entered on a line by themselves:
        |    help - Prints this help message.
        |    quit - Exits the terminal.
        |
        |All other commands are statements in the layout definition language.
        |These commands must be terminated by a ';' character. They may span
        |multiple lines if required. e.g.:
        |
        |  schema> SHOW TABLES;
        |  schema> DESCRIBE foo;
        |  schema> ALTER TABLE foo SET DESCRIPTION =
        |       ->    'A table\'s worth of data';
        |
        |Keywords are not case sensitive. Table and column names are. You may
        |enclose table and column names in 'single quotes' if desired (e.g., if you
        |use a keyword as a table name.). Strings like a description string must be
        |'single quoted.' You should escape internal single-quote marks with a
        |leading backslash, as in the example above.
        |
        |AVAILABLE COMMANDS:
        |  SHOW INSTANCES;
        |
        |  USE <instance>;
        |  CREATE INSTANCE <instance>;
        |  DROP INSTANCE <instance>;
        |
        |  SHOW TABLES;
        |
        |  DESCRIBE <table>;
        |  DESCRIBE EXTENDED <table>;
        |  DESCRIBE <table> COLUMN info:foo SHOW [n] { READER | WRITER | RECORDED } SCHEMAS;
        |
        |  DROP TABLE <table>;
        |  CREATE TABLE <name> WITH DESCRIPTION 'description'
        |    ROW KEY FORMAT { HASHED | RAW | HASH PREFIXED(n) | (formatted--see below*) }
        |    [PROPERTIES (
        |      MAX FILE SIZE = <n>,
        |      MEMSTORE FLUSH SIZE = <n>,
        |      NUMREGIONS = <n>,
        |      VALIDATION = { NONE | LEGACY | DEVELOPER | STRICT }
        |    )]
        |    WITH LOCALITY GROUP <group> WITH DESCRIPTION 'description' (
        |      MAXVERSIONS = <n>,
        |      INMEMORY = { true | false },
        |      TTL = <n>,
        |      COMPRESSED WITH { GZIP | LZO | SNAPPY | NONE },
        |      BLOOM FILTER = { NONE | ROW | ROWCOL },
        |      BLOCK SIZE = { NULL | <n> },
        |      GROUP TYPE FAMILY <family> WITH DESCRIPTION 'description' (
        |        COLUMN <column> [WITH SCHEMA schema] WITH DESCRIPTION 'desc'
        |      ),
        |      MAP TYPE FAMILY <family> [WITH SCHEMA schema] WITH DESCRIPTION 'desc'),
        |    WITH LOCALITY GROUP...;
        |
        |  ... "Formatted" (composite) row keys can be used in CREATE TABLE as follows:
        |  ROW KEY FORMAT (
        |        component_name { [STRING] | INT | LONG } [NOT NULL],
        |        component_name ...,
        |        [HASH ([THROUGH component_name], [SIZE = n], [SUPPRESS FIELDS])])
        |
        |  ALTER TABLE t ADD COLUMN info:foo [WITH SCHEMA] schema [WITH DESCRIPTION 'd'];
        |  ALTER TABLE t RENAME COLUMN info:foo [AS] info:bar;
        |  ALTER TABLE t DROP COLUMN info:foo;
        |
        |  ALTER TABLE t ADD [GROUP TYPE] FAMILY f [WITH DESCRIPTION 'd'] TO [LOCALITY GROUP] lg;
        |  ALTER TABLE t ADD MAP TYPE FAMILY f [WITH SCHEMA] schema
        |      [WITH DESCRIPTION 'd'] TO [LOCALITY GROUP] lg;
        |
        |  ALTER TABLE t DROP FAMILY f;
        |  ALTER TABLE t RENAME FAMILY f [AS] f2;
        |
        |  ALTER TABLE t CREATE LOCALITY GROUP lg [WITH DESCRIPTION 'd']
        |      [( property, property... )]
        |
        |  ... Where 'property' is one of:
        |        MAXVERSIONS = int
        |      | INMEMORY = bool
        |      | TTL = int
        |      | COMPRESSED WITH { GZIP | LZO | SNAPPY | NONE }
        |  ... or a FAMILY definition (see the earlier section on the CREATE TABLE syntax)
        |
        |  ALTER TABLE t RENAME LOCALITY GROUP lg [AS] lg2;
        |  ALTER TABLE t DROP LOCALITY GROUP lg;
        |
        |  ALTER TABLE t SET DESCRIPTION = 'desc';
        |  ALTER TABLE t SET DESCRIPTION = 'desc' FOR FAMILY f;
        |  ALTER TABLE t SET DESCRIPTION = 'desc' FOR COLUMN info:foo;
        |  ALTER TABLE t SET DESCRIPTION = 'desc' FOR LOCALITY GROUP lg;
        |
        |  ALTER TABLE t ADD [ [DEFAULT] READER | WRITER ] SCHEMA schema FOR [MAP TYPE] FAMILY f;
        |  ALTER TABLE t ADD [ [DEFAULT] READER | WRITER ] SCHEMA schema FOR COLUMN info:foo;
        |  ALTER TABLE t DROP [ [DEFAULT] READER | WRITER ] SCHEMA schema FOR [MAP TYPE] FAMILY f;
        |  ALTER TABLE t DROP [ [DEFAULT] READER | WRITER ] SCHEMA schema FOR COLUMN info:foo;
        |
        |  ... Where 'schema' is one of:
        |       CLASS com.example.FooRecord
        |     | COUNTER
        |     | ID <schema-id> (see DESCRIBE table COLUMN info:foo..)
        |     | <schema JSON>
        |
        |  (deprecated) ALTER TABLE t SET SCHEMA = schema FOR [MAP TYPE] FAMILY f;
        |  (deprecated) ALTER TABLE t SET SCHEMA = schema FOR COLUMN info:foo;
        |
        |  ALTER TABLE t SET property FOR LOCALITY GROUP lg;
        |  ... where 'property' is one of MAXVERSIONS, INMEMORY, TTL, or COMPRESSED WITH.
        |
        |  DUMP DDL [TO FILE '/path/to/foo.ddl'] [FOR TABLE <table>];
        |  LOAD FROM FILE '/path/to/foo.ddl';
        |
        |  USE JAR INFILE '/path/to/resource.jar';
        |
        |  MODULE 'modulename';
        |  SHOW MODULES;
        |
        |For a full layout definition language reference, see the user guide
        |available online at:
        |""".stripMargin +
        "  http://docs.kiji.org/userguides/schema/" +
        org.kiji.schema.util.VersionInfo.getSoftwareVersion() + "/schema-shell-ddl-ref/\n" +
        """
        |For more information,see the README.md file distributed with this program.
        |If you got this in a BentoBox, this is in the 'schema-shell' directory.
        |""".stripMargin

    val moduleHelpLines : String = env.modules.foldLeft("")({
      (helpLines: String, module: ParserPluginFactory) =>
        if (module.isInstanceOf[HelpPlugin]) {
          val moduleWithHelp: HelpPlugin = module.asInstanceOf[HelpPlugin]
          helpLines + """
            |MODULE """.stripMargin + moduleWithHelp.getName + """:
            |
            |""".stripMargin + moduleWithHelp.helpText() + "\n"
        } else {
          helpLines
        }
    })

    printPages(helpLines + moduleHelpLines, env)
  }

  /**
   * Print a large amount of text to the screen page-by-page.
   *
   * @param text the lines of text to print
   * @param env - the current operating environment.
   */
  private def printPages(text: String, env: Environment): Unit = {
   val terminal = new UnixTerminal
   val termHeight = terminal.getTerminalHeight() - 1
   // Default page height of 24 lines if can't determine from stty.
   val pageHeight = if (termHeight < 1) { 24 } else { termHeight };
   var i = 0;
   text.lines.foreach { line =>
     if (i == pageHeight) {
       env.inputSource.readLine("<more>")
       i = 0
     }
     env.printer.println(line)
     i = i + 1
   }
  }

  /**
   * Request a line of user input, parse it, and execute the command.
   * If this is an exit/quit call, exit this process.
   * Otherwise, recursively continue to request the next line of user input.
   *
   * @param buf - the input command so far (from previous input lines)
   * @param env - the current operating environment
   * @return the final environment.
   */
  def processUserInput(buf: StringBuilder, env: Environment): Environment = {
    val prompt = if (buf.length() > 0) CONTINUE_PROMPT_STR else PROMPT_STR
    env.inputSource.readLine(prompt) match {
      case None => { env /* Out of input. Return success. */ }
      case Some(inputLine) => {
        Log.debug("Processing input line: '{}'", inputLine)
        buf.append(inputLine).append('\n')  // JLine does not include the end-of-line '\n'

        val inputSoFar: String = buf.toString.trim

        if (Set("exit", "quit", "exit;", "quit;").contains(inputSoFar)) {
          env
        } else if (Set("help", "help;").contains(inputSoFar)) {
          printHelp(env)
          processUserInput(new StringBuilder, env)
        } else if (inputSoFar.endsWith(";")) {
          val parser = new DDLParser(env)
          try {
            val parseResult = parser.parseAll(parser.statement, inputSoFar)
            val nextEnv = (
              try {
                parseResult.getOrElse(new ErrorCommand(
                    env, parseResult.toString(), throwOnErr)).exec()
              } catch {
                case e: DDLException => {
                  println(e.getMessage())
                  if (throwOnErr) {
                    throw e
                  }
                  env
                }
                case ioe: IOException => {
                  println(ioe.getMessage())
                  if (throwOnErr) {
                    throw ioe
                  }
                  env
                }
              }
            )
            // Continue processing with a new input buffer.
            processUserInput(new StringBuilder, nextEnv)
          } catch {
            case e: KijiInvalidNameException =>
              println("Invalid identifier: " + e.getMessage())
              if (throwOnErr) {
                throw new DDLException("Invalid identifier: " + e.getMessage())
              }
              processUserInput(new StringBuilder, env)
          }
        } else {
          // More input needed:
          processUserInput(buf, env)
        }
      }
    }
  }
}
