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

package org.kiji.express.flow

import java.io.File
import java.io.FileWriter
import java.io.FileInputStream
import java.io.BufferedWriter
import scala.collection.JavaConverters._
import scala.collection.mutable

import cascading.pipe.Pipe
import cascading.flow.FlowDef
import cascading.pipe.Each
import cascading.tuple.Fields
import com.google.common.io.Files
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.Hdfs
import com.twitter.scalding.TupleConversions
import com.twitter.scalding.TupleSetter
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.SideEffectMapFunction
import com.twitter.scalding.Args
import com.twitter.scalding.RichPipe
import com.twitter.scalding.IterableSource
import org.apache.avro.util.Utf8
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.express.KijiSuite
import org.kiji.schema.KijiRowKeyComponents
import org.kiji.mapreduce.kvstore.KeyValueStore
import org.kiji.mapreduce.kvstore.KeyValueStoreReader
import org.kiji.mapreduce.kvstore.impl.XmlKeyValueStoreParser
import org.kiji.mapreduce.kvstore.lib.InMemoryMapKeyValueStore
import org.kiji.mapreduce.kvstore.lib.TextFileKeyValueStore
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.api.Client
import org.kiji.schema.util.InstanceBuilder

trait TestPipeConversions {
  implicit def pipe2TestPipe(pipe: Pipe): TestPipe = new TestPipe(pipe)
}

object TestPipe {
  val logger: Logger = LoggerFactory.getLogger(classOf[KeyValueStoreSuite])
}
class TestPipe(val pipe: Pipe)
    extends FieldConversions
    with TupleConversions
    with TestPipeConversions
    with Serializable
{
  implicit def pipeToRichPipe(pipe : Pipe): RichPipe = new RichPipe(pipe)

  def assertOutputValues[A](
      fields: Fields,
      expected: Set[A]
  )(
      implicit conv: TupleConverter[A],
      set: TupleSetter[Unit],
      flowDef: FlowDef,
      mode: Mode
  ): Unit = {
    conv.assertArityMatches(fields)

    def bf: mutable.Set[A] = mutable.Set[A]()

    def ef(mySet: mutable.Set[A]): Unit = {
      val output: Set[A] = mySet.toSet

      if (expected == output) {
        TestPipe.logger.debug("Confirmed values on pipe!")
      } else {
        TestPipe.logger.debug("Mismatch in expected value for pipe and output value")

        val inExpectedNotFound: Set[A] = expected -- output
        TestPipe.logger
            .debug("Values in expected output that were not found: " + inExpectedNotFound)

        val foundButNotExpected: Set[A] = output -- expected
        TestPipe.logger.debug("Values found but not expected: " + foundButNotExpected)

        throw new Exception("MISMATCH!")
      }
    }

    def fn(mySet: mutable.Set[A], tup: A): Unit = { mySet += tup }

    val newPipe = new Each(
        pipe,
        fields,
        new SideEffectMapFunction(bf, fn, ef, Fields.NONE, conv, set))
    NullSource.writeFrom(newPipe)(flowDef, mode)
  }
}

@RunWith(classOf[JUnitRunner])
class KeyValueStoreSuite extends KijiSuite {
  val logger: Logger = LoggerFactory.getLogger(classOf[KeyValueStoreSuite])

  /**
   * Utility method to build a Kiji table for testing.  Should eventually go into a KijiExpress
   * testing library.
   */
  def createTableAndPopulateTableAndReturnUri(
      ddl: String,
      tableName: String,
      functionToPopulateTable: InstanceBuilder#TableBuilder => Unit,
      instanceName: String = "default_%s".format(counter.incrementAndGet())
  ): String = {

    val kiji: Kiji = new InstanceBuilder(instanceName).build()
    try {
      // Create the instance
      val kijiUri: KijiURI = kiji.getURI()

      val client: Client = Client.newInstance(kijiUri)
      client.executeUpdate(ddl)
      client.close()

      val table: KijiTable = kiji.openTable(tableName)
      try {
        // Populate the table!!!!
        functionToPopulateTable(new InstanceBuilder(kiji).withTable(table))

        val uri: String = table.getURI().toString
        return uri
      } finally {
        table.release()
      }
    } finally {
      kiji.release()
    }
  }

  test("KeyValueStores with KijiTables and in-memory maps work") {

    // Ensure that the mode is what we need.
    Mode.mode = Hdfs(strict = false, conf = HBaseConfiguration.create())

    //--------------------------------------------------------------------------------
    // Create a KijiTable of users and their favorite fruits
    //
    val fruitDDL: String = """
      CREATE TABLE fruitTable
      WITH DESCRIPTION 'Everyone loves fruit!'
      ROW KEY FORMAT (user STRING)
      WITH LOCALITY GROUP default (
        FAMILY family ( COLUMN column WITH SCHEMA "string" ));
    """

    def populatefruit(tableBuilder: InstanceBuilder#TableBuilder): Unit = {
      tableBuilder
        .withRow("alice")
        .withFamily("family")
        .withQualifier("column").withValue("apple,banana")
        .withRow("bob")
        .withFamily("family")
        .withQualifier("column").withValue("cantelope")
        .withRow("charles")
        .withFamily("family")
        .withQualifier("column").withValue("kiwi,watermelon")
        .withRow("deborah")
        .withFamily("family")
        .withQualifier("column").withValue("pear")
        .build()
    }

    val fruitUri: String  = createTableAndPopulateTableAndReturnUri(
        fruitDDL, "fruitTable", populatefruit)

    //--------------------------------------------------------------------------------
    // Create a KijiTable of users and their home cities
    //
    val cityUri: String  = createTableAndPopulateTableAndReturnUri(
        ddl = """
          CREATE TABLE CityTable
          WITH DESCRIPTION 'Everyone has to live somewhere!'
          ROW KEY FORMAT (user STRING)
          WITH LOCALITY GROUP default (
            FAMILY family ( COLUMN city WITH SCHEMA "string" ));""",
        tableName = "CityTable",
        functionToPopulateTable = (tableBuilder: InstanceBuilder#TableBuilder) => { tableBuilder
          .withRow("alice")
          .withFamily("family")
          .withQualifier("city").withValue("San Francisco")
          .withRow("bob")
          .withFamily("family")
          .withQualifier("city").withValue("Oakland")
          .withRow("charles")
          .withFamily("family")
          .withQualifier("city").withValue("San Jose")
          .build()
        })
    val jobTest =
        new KeyValueStoreSuite.KvsFruitJob(Args("--fruit " + fruitUri + " --city " + cityUri))
    jobTest.run
  }

  test("KeyValueStore works with delimited file.") {

    // Ensure that the mode is what we need.
    Mode.mode = Hdfs(strict = false, conf = HBaseConfiguration.create())

    val outputDir = Files.createTempDir()
    val csvFile = new File(outputDir.getAbsolutePath + "/nba.csv")
    val bw = new BufferedWriter(new FileWriter(csvFile))
    bw.write("""
        |Jordan,Bulls
        |Barkley,Suns
        |Ewing,Knicks
        |Malone,Jazz
        |Magic,Lakers
        |Bird,Celtics""".stripMargin)
    bw.close()
    val jobTest = new KeyValueStoreSuite.KvsNbaJob(Args("--nba-csv " + csvFile.getAbsolutePath))
    jobTest.run
    FileUtils.deleteDirectory(outputDir)
  }

  test("KeyValueStore works with XML configuration file.") {

    // Ensure that the mode is what we need.
    Mode.mode = Hdfs(strict = false, conf = HBaseConfiguration.create())

    //--------------------------------------------------------------------------------
    // Create a KijiTable of NBA players and their teams
    //
    val nbaDDL: String =
        """CREATE TABLE nbaTable
          |WITH DESCRIPTION 'NBA players and the teams for which they are best remembered'
          |ROW KEY FORMAT (player STRING)
          |WITH LOCALITY GROUP default (
          |  FAMILY info (
          |    COLUMN team WITH SCHEMA "string"
          |  ));""".stripMargin

    def populateNba(tableBuilder: InstanceBuilder#TableBuilder): Unit = {
      tableBuilder
        .withRow("Jordan")
        .withFamily("info")
        .withQualifier("team").withValue("Bulls")
        .withRow("Magic")
        .withFamily("info")
        .withQualifier("team").withValue("Lakers")
        .build()
    }

    val nbaUri: String  = createTableAndPopulateTableAndReturnUri(
        nbaDDL, "nbaTable", populateNba)

    // Create XML file that describes KVS
    val outputDir = Files.createTempDir()
    val xmlFile = new File(outputDir.getAbsolutePath + "/nba-kvs.xml")
    val bwXml = new BufferedWriter(new FileWriter(xmlFile))
    bwXml.write("""
        | <stores>
        |   <store class="org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore" name="nba-kvs">
        |     <configuration>
        |       <property>
        |         <name>table.uri</name>
        |         <value>%s</value>
        |       </property>
        |       <property>
        |         <name>column</name>
        |         <value>info:team</value>
        |       </property>
        |     </configuration>
        |   </store>
        | </stores>""".format(nbaUri).stripMargin)
    bwXml.close()

    val jobTest = new KeyValueStoreSuite.KvsNbaJob2(Args("--kvs-xml " + xmlFile.getAbsolutePath))
    jobTest.run
    FileUtils.deleteDirectory(outputDir)
  }
}

object KeyValueStoreSuite {
  // Simple Express job that uses a KVS taken from a CSV file on HDFS.
  class KvsNbaJob(args: Args) extends KijiJob(args) with TestPipeConversions {
    def createKeyValueStoreContext: ExpressKeyValueStore[String, String] = {
      val reader = TextFileKeyValueStore
          .builder()
          .withDelimiter(",")
          .withDistributedCache(true)
          .withInputPath(new Path(args("nba-csv")))
          .build()
          .open()
      ExpressKeyValueStore[String, String](reader)
    }

    IterableSource(List("Jordan", "Barkley", "Ewing", "Malone", "Magic", "Bird"), 'player)
        .using(createKeyValueStoreContext)
            .map('player -> 'team) { (kvs: ExpressKeyValueStore[String, String], player: String) =>
              kvs.getOrElse(player, "No team!")
            }
        .assertOutputValues(
            ('player, 'team),
            Set(
                ("Jordan", "Bulls"),
                ("Barkley", "Suns"),
                ("Ewing", "Knicks"),
                ("Malone", "Jazz"),
                ("Magic", "Lakers"),
                ("Bird", "Celtics")
            )
        )
  }

  /**
   * Simple Express job that uses KeyValueStores to perform some silly joins across customers,
   * customers' favorite fruits, and customers' home cities.
   */
  class KvsFruitJob(args: Args) extends KijiJob(args) with TestPipeConversions {

    val fruitPrices: Map[String, String] = Map(
        "APPLE" -> "$1",
        "BANANA" -> "$2",
        "CANTELOPE" -> "$50", // Cantelopes are expensive this year!!!!!!!
        "KIWI" -> "$1",
        "WATERMELON" -> "$2",
        "PEAR" -> "$3"
    )

    /**
     * Create a reference to an Express KeyValueStore for reading the prices of fruits from an
     * in-memory KVS.  This function uses a more verbose version of the factory method.
     *
     * The in-memory KVS uses key and value converter methods.
     *
     *   For keys, the fruit names in the "fruit" table maybe have fruits in any case, but in the
     *   prices table, the fruit names are ALL CAPS.
     *
     *   For values, the prices of fruits are stored as strings (e.g., "$0.49") that need to get
     *   converted into Doubles.
     *
     * This example is silly, but hopefully illustrates the usefulness of having keyConverter and
     * valueConverter methods.
     *
     * @return An `ExpressKeyValueStore` for the `Map` defined above.
     */
    def createPriceKeyValueStoreContext: ExpressKeyValueStore[String, Int] = {
      ExpressKeyValueStore[String, Int, String, String](
          InMemoryMapKeyValueStore.fromMap(fruitPrices.asJava).open,
          keyConverter = (key: String) => key.toUpperCase,
          valueConverter = (value: String) => value.substring(1).toInt
      )
    }

    /**
     * Create a reference to an Express KeyValueStore for reading users' home cities.  This
     * function uses a very concise factory method.
     *
     * @return An `ExpressKeyValueStore` for reading data from the "family:city" column.
     */
    def createCityKeyValueStoreContext: ExpressKeyValueStore[EntityId, String] = {
      // Call to Express KeyValueStore interface!
      ExpressKijiTableKeyValueStore[String, Utf8](
          tableUri = args("city"),
          column = "family:city",
          // Avro serializes strings as Utf8, so we use a "valueConverter" function here to
          // convert the values to Strings.
          valueConverter = (value: Utf8) => value.toString
      )
    }

    /*
      Here is the actual flow that we want to execute!
      We have two input tables:

        args("fruit"):
        - entityId is user name
        - family:column contains the users favorite fruits

        args("city")
        - entityId is user name
        - family:city is the user's home city
    */
    KijiInput.builder
        .withTableURI(args("fruit"))
        .withTimeRangeSpec(TimeRangeSpec.All)
        .withColumns("family:column" -> 'slice)
        .build
        .flatMap('slice -> 'fruit) { slice: Seq[FlowCell[CharSequence]] =>
          slice.head.datum.toString.split(",").toList
        }

        // Get the user names from the entity IDs
        .map('entityId -> 'user) { eid: EntityId => eid.components(0).toString }
        .project('entityId, 'user, 'fruit)

        //----------------------------------------------------------------------------------------
        // Use the key value store to also get the user's city!
        .using(createCityKeyValueStoreContext)
            // KVS available for this map command
            .map('entityId -> 'city) {
              (kvs: ExpressKeyValueStore[EntityId, String], eid: EntityId) => {
                kvs.getOrElse(eid, "No city!!!")
              }
            }
            //...KVS no longer available, Scalding will automatically call the "close" method

        //----------------------------------------------------------------------------------------
        // Use another key value store to get fruit prices!
        .using(createPriceKeyValueStoreContext)
            .map('fruit -> 'price) { (kvs: ExpressKeyValueStore[String, Int], fruit: String) =>
              kvs(fruit)
            }
        .assertOutputValues(
            ('user, 'fruit, 'city, 'price),
            Set(
                ("alice", "apple", "San Francisco", 1),
                ("alice", "banana", "San Francisco", 2),
                ("bob", "cantelope", "Oakland", 50),
                ("charles", "kiwi", "San Jose", 1),
                ("charles", "watermelon", "San Jose", 2),
                ("deborah", "pear", "No city!!!", 3)
            )
        )
  }

  // Express job that uses an XML file to define KeyValueStores.
  class KvsNbaJob2(args: Args) extends KijiJob(args) with TestPipeConversions {

    // Get a map from KVS names to KVS objects
    def kvsMap: mutable.Map[String, KeyValueStore[_, _]] = {
      val map = XmlKeyValueStoreParser
          .get(new Configuration())
          .loadStoresFromXml(new FileInputStream(args("kvs-xml"))).asScala

      // Quick sanity checks
      assert(map.size == 1)
      assert(map.contains("nba-kvs"))

      map
    }

    def createKeyValueStoreContext: ExpressKeyValueStore[String, String] = {
      val reader: KeyValueStoreReader[KijiRowKeyComponents, Utf8] = kvsMap("nba-kvs")
          .open()
          .asInstanceOf[KeyValueStoreReader[KijiRowKeyComponents, Utf8]]

      ExpressKeyValueStore[String, String, KijiRowKeyComponents, Utf8](
        kvStoreReader = reader,
        keyConverter = (player: String) => KijiRowKeyComponents.fromComponents(player),
        valueConverter = (team: Utf8) => team.toString
      )
    }

    IterableSource(List("Jordan", "Magic"), 'player)
      .using(createKeyValueStoreContext)
      .map('player -> 'team) {
      (kvs: ExpressKeyValueStore[String, String], player: String) =>
        kvs.getOrElse(player, "No team!") }
      .assertOutputValues(('player, 'team), Set(("Jordan", "Bulls"), ("Magic", "Lakers")))
  }
}
