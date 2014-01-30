---
layout: post
title: Running KijiExpress Jobs
categories: [userguides, express, 1.0.1]
tags : [express-ug]
version: 1.0.1
order : 8
description: Running KijiExpress Jobs.
---

There are two ways to run KijiExpress logic: compiled into Jobs or in the KijiExpress language shell (REPL).

## Running Compiled Jobs

KijiExpress programs are written in Scala with KijiExpress libraries included as dependencies. The Kiji project includes configuration for Maven that eases building these Scala files into runnable Java JARs. You can choose to compile KijiExpress Scala files with whatever system you are comfortable with; see Build Tools for setting up the Kiji project dependencies.

To produce KijiExpress code that can be compiled:

* Wrap your logic in a KijiJob class.

        class SomeClassName(args: Args) extends KijiJob(args) {
            // include KijiExpress functions and pipeline operations here
        }

    Where Args is an object that allows you to easily pass in command-line arguments.

    Typically arguments will be expressed on the command-line with   option-name option-value and can be accessed within the job as args("option-name"). Usually this is done with an input such as a reference to a Kiji table: KijiInput(args("table-name")).

* Import the dependencies required by the logic.

    Typically, you’ll need the following:

        import com.twitter.scalding._
        import org.kiji.express._
        import org.kiji.express.flow._

    In Scala, the underscore is a wildcard character that indicates all files in the given location.

The command to run a compiled KijiExpress job is as follows:

    express job --libjars <path/to/dependencies> \
        <path/to/jar.jar> \
        <class to run> \
        --input <path/to/source/files-tables> \
        --table-uri <URI of Kiji table>
        --output <path/to/target/files-tables>
        --hdfs

Your KijiExpress job can access additional arguments that can be specified on the command line. The
standard arguments are as follows:

jar file
:Path to the JAR file that includes the class you want to run.

`--libjars \<path\>`
:Path to compiled JAR files that include auxiliary jars that need to be included. There
is a space after the option name, then the path follows in double quotation marks.

`--hdfs`
:Specifies that the job is to look for input and write output to the HDFS on the running
cluster. Omit this option to run in “local” mode where input and output files are expected
relative to the location where the command is run.

For example, to refer to external jars for the music tutorial project, an express command
line would include:

    express script examples/express-music/scripts/SongMetadataImporter.express \
    --libjars “examples/express-music/lib/*” --hdfs

## Running Logic in the KijiExpress Shell

KijiExpress includes a REPL or command line shell where you can run one or more Scala
statements and KijiExpress will evaluate the results.

Anything you can do in an KijiExpress flow you can do in the REPL. The REPL is particularly
useful for development and prototyping. For example, you might write and test a flow for a
training procedure in the REPL, and then later use that prototype to author an actual train
implementation by wrapping everything up in a new class that implements the training procedure.

Commands specific to the shell which should not be evaluated by the Scala interpreter are
prefixed with a colon (:). Here’s a sample interactive session:

    $ cd <path/to/project>
    $ express shell
    express> :paste
    // Entering paste mode (ctrl-D to finish)
    import scala.util.parsing.json.JSON
    def parseJson(json: String): (String, String, String, String, String, Long, Long) = {
        val metadata = JSON.parseFull(json).get.asInstanceOf[Map[String, Any]]
        (metadata.get("song_id").get.asInstanceOf[String],
            metadata.get("song_name").get.asInstanceOf[String],
            metadata.get("album_name").get.asInstanceOf[String],
            metadata.get("artist_name").get.asInstanceOf[String],
            metadata.get("genre").get.asInstanceOf[String],
            metadata.get("tempo").get.asInstanceOf[String].toLong,
            metadata.get("duration").get.asInstanceOf[String].toLong)
      }

      TextLine("express-tutorial/song-metadata.json")
          .map('line ->
              ('songId, 'songName, 'albumName, 'artistName, 'genre, 'tempo,'duration)) { parseJson }
          .map('songId -> 'entityId) { songId: String => EntityId(songId) }
          .packAvro(('songName, 'albumName, 'artistName, 'genre, 'tempo, 'duration)
              -> 'metadata)
          .write(KijiOutput(args("table-uri"))('metadata -> "info:metadata"))

    // Existing paste mode, now interpreting.
    //<typical output here>

Alternatively you can simply point to the Scala file:

    $ cd <path/to/project>
    $ express shell
    express> :load SongMetadataImporter.scala


* `: help` lists the shell commands.

* Don’t use forward slash (/) to indicate line breaks; instead use Scala conventions for
where you can put breaks. (Long lines work also.)

* Make sure to remove any constructs that are aimed at a compiled job: for example, the
`KijiJob` class wrapper and the args parameter for functions.

* More than one blank line in a row in the Scala input (from `:paste` or `:load`) will fail.

* You can also run KijiSchema commands from the KijiExpress shell by using the `:schema-shell`
  command.  From inside the shell, type

        :help

   for a list of meta-commands.  These are the same as the regular scala interpreter, with the
addition of the command `:schema-shell` which will drop you into an instance of the KijiSchema
shell.
