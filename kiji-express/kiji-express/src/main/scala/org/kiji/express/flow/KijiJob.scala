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

import java.io.Serializable
import java.util.Properties

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

import cascading.flow.Flow
import cascading.flow.FlowListener
import cascading.flow.hadoop.util.HadoopUtil
import cascading.pipe.Checkpoint
import cascading.pipe.Pipe
import cascading.pipe.assembly.AggregateBy
import cascading.stats.FlowStepStats
import cascading.stats.hadoop.HadoopStepStats
import cascading.stats.local.LocalStepStats
import cascading.tap.Tap
import cascading.tuple.collect.SpillableProps
import com.google.common.base.Preconditions
import com.twitter.chill.config.ConfiguredInstantiator
import com.twitter.chill.config.ScalaAnyRefMapConfig
import com.twitter.scalding.Args
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.mapred.Counters
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RunningJob
import org.apache.hadoop.mapreduce.Counter
import org.apache.hadoop.security.UserGroupInformation
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.framework.ExpressJobHistoryKijiTable
import org.kiji.express.flow.framework.KijiTap
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.flow.framework.hfile.HFileFlowStepStrategy
import org.kiji.express.flow.framework.hfile.HFileKijiTap
import org.kiji.express.flow.framework.serialization.KijiKryoInstantiator
import org.kiji.express.flow.histogram.HistogramConfig
import org.kiji.express.flow.histogram.TupleProfiling
import org.kiji.express.flow.util.AvroTupleConversions
import org.kiji.express.flow.util.PipeConversions
import org.kiji.express.flow.util.ResourcesShutdown
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * KijiJob is KijiExpress's extension of Scalding's `Job`, and users should extend it when writing
 * their own jobs in KijiExpress.  It provides extra conversions that Express needs for KijiPipes.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.  Within your own
 *     KijiJob, `args("input")` will evaluate to "SomeFile.txt" if your command line contained the
 *     argument `--input SomeFile.txt`
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
class KijiJob(args: Args)
    extends Job(args)
    with PipeConversions
    with AvroTupleConversions {
  import KijiJob._

  /** FlowListener for collecting flowCounters from this Job. */
  private val counterListener: CounterListener = new CounterListener

  // We have to reference ResourcesShutdown from here, or else the class never gets loaded into
  // the jvm.
  ResourcesShutdown.initialize()

  override def buildFlow: Flow[_] = {
    val taps: List[Tap[_, _, _]] = (
        flowDef.getSources.values.asScala.toList
            ++ flowDef.getSinks.values.asScala.toList
            ++ flowDef.getCheckpoints.values.asScala.toList)

    // Retrieve the configuration
    val conf: Configuration = HBaseConfiguration.create()
    mode match {
      case Hdfs(_, configuration) => {
        HBaseConfiguration.merge(conf, configuration)

        // Obtain any necessary tokens for the current user if security is enabled.
        if (User.isHBaseSecurityEnabled(conf)) {
          val user = UserGroupInformation.getCurrentUser
          if (user.getTokens == null || user.getTokens.isEmpty) {
            TokenUtil.obtainAndCacheToken(conf, user)
          }
        }
      }
      case HadoopTest(configuration, _) => {
        HBaseConfiguration.merge(conf, configuration)
      }
      case _ =>
    }

    // Validate that the Kiji parts of the sources (tables, columns) are valid and exist.
    taps.foreach {
      case tap: KijiTap => tap.validate(conf)
      case tap: HFileKijiTap => tap.validate(conf)
      case tap: LocalKijiTap => {
        val properties: Properties = new Properties()
        properties.putAll(HadoopUtil.createProperties(conf))
        tap.validate(properties)
      }
      case _ => // No Kiji parts to verify.
    }

    // Handle HFile writes.
    checkpointHFileSink()
    val flow = super.buildFlow
    // Here we set the strategy to change the sink steps since we are dumping to HFiles.
    flow.setFlowStepStrategy(HFileFlowStepStrategy)
    flow
  }

  /**
   * Modifies the flowDef to include an explicit checkpoint when writing HFiles, if necessary.
   * Checkpoints are necessary when the final stage of the job writing to an HFile tap includes a
   * reducer, i.e., if it is not a map-only stage.
   */
  private def checkpointHFileSink(): Unit = {
    val sinks: java.util.Map[String, Tap[_, _, _]] = flowDef.getSinks
    val tails: java.util.List[Pipe] = flowDef.getTails

    val hfileSinks = sinks.asScala.collect { case (name, _: HFileKijiTap) => name }.toSet

    if (!hfileSinks.isEmpty) {
      val tailsMap = flowDef.getTails.asScala.map((p: Pipe) => p.getName -> p).toMap
      val flow: Flow[JobConf] = super.buildFlow.asInstanceOf[Flow[JobConf]]
      for {
        flowStep <- flow.getFlowSteps.asScala
        sink <- flowStep.getSinks.asScala
        name <- flowStep.getSinkName(sink).asScala if hfileSinks(name)
      } {
        if (flowStep.getConfig.getNumReduceTasks > 0) {
          // insert checkpoint to force writing the tuples to a temp file inHDFS. The subsequent
          // reading of the checkpoint and tuples flowing into the HFileTap will be map-only and
          // hence allows the IdentityReducer + TotalOrderPartitioner to properly sink the values to
          // HFiles
          val tail = tailsMap(name)
          tails.remove(tail)
          flowDef.addTail(new Pipe(name, new Checkpoint(tail.getPrevious.head)))
        }
      }
    }
  }

  override def config: Map[AnyRef, AnyRef] = {
    val baseConfig: Map[AnyRef, AnyRef] = super.config

    // We configure as is done in Scalding's Job, but then append to mapred.child.java.opts to
    // disable schema validation. This system property is only useful for KijiSchema v1.1. In newer
    // versions of KijiSchema, this property has no effect.
    val disableValidation = " -Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED"
    val oldJavaOptions: String =
      baseConfig.getOrElse("mapred.child.java.opts", "").asInstanceOf[String]

    // These are ignored if set in mode.config
    val lowPriorityDefaults = Map(
        SpillableProps.LIST_THRESHOLD -> defaultSpillThreshold.toString,
        SpillableProps.MAP_THRESHOLD -> defaultSpillThreshold.toString,
        AggregateBy.AGGREGATE_BY_THRESHOLD -> defaultSpillThreshold.toString
    )
    // Set up the keys for chill
    val chillConf = ScalaAnyRefMapConfig(lowPriorityDefaults)
    ConfiguredInstantiator.setReflect(chillConf, classOf[KijiKryoInstantiator])

    val oldTmpJars: Option[String] =
      baseConfig.get(tmpjarsConfigProperty).asInstanceOf[Option[String]]
    val userSpecifiedTmpJars: Option[String] = args.optional(tmpjarsArg)
    // If addClasspathProperty is true, include the classpath in "tmpjars". Default to true.
    val addClasspath: Boolean =
      args.optional(addClasspathArg).getOrElse("true").toBoolean
    val classpathTmpJars: Option[String] = if (addClasspath) {
      classpathJars()
    } else {
      None
    }
    val newTmpJars: String = (oldTmpJars ++ userSpecifiedTmpJars ++ classpathTmpJars).mkString(",")
    val tmpJarsMap: Map[String, String] =
      if (newTmpJars.isEmpty) {
        Map[String, String]()
      } else {
        Map(tmpjarsConfigProperty -> newTmpJars)
      }

    // Append all the new keys.
    baseConfig ++
        chillConf.toMap +
        ("mapred.child.java.opts" -> (oldJavaOptions + disableValidation)) ++
        tmpJarsMap
  }

  /**
   * Get the flowCounters from this job. Will be empty until the job completes. If `listeners` is
   * overridden without concatenating `super.listeners`, flowCounters will not be recorded.
   *
   * @return The set of flowCounters from this job. (CounterGroup, CounterName, Count).
   */
  private[express] def flowCounters: Set[(String, String, Long)] = counterListener.flowCounters

  /**
   * Get the counters for the different flowSteps for this job.
   * Will be empty until the job completes.
   *
   * @return The set of flowCounters from this job. (CounterGroup, CounterName, Count).
   */
  private[express] def flowStepCounters: Iterable[Set[(String, String, Long)]] =
    counterListener.flowStepCounters

  /**
   * Override this to add custom listeners.
   *
   * The List of custom listeners should be concatenated to `super.listeners`.
   *
   * @return a List of custom listeners concatenated to `super.listeners`.
   */
  override def listeners: List[FlowListener] = {
    counterListener :: super.listeners
  }

  /**
   * Override this to add custom histograms. For registered histograms to be printed when specifying
   * custom listeners, the desired custom listeners must be concatenated with `super.listeners`.
   *
   * @return a List of custom histograms that are used in this job.
   */
  def histograms: List[HistogramConfig] = List()

  /**
   * Record the history of this job into all relevant Kiji instances. This includes all Kiji
   * instances which are read from or written to by flows in this Job.
   *
   * @param startTime in milliseconds since the epoch at which the job started.
   * @param endTime in milliseconds since the epoch at which the job ended.
   * @param jobSuccess whether the job was successful.
   */
  private def recordJobHistory(startTime: Long, endTime: Long, jobSuccess: Boolean) {
    def maybeGetTableUri(t: Tap[_, _, _]): Option[String] = {
      t match {
        case kt: KijiTap => Some(kt.tableUri)
        case lkt: LocalKijiTap => Some(lkt.tableUri)
        case hfkt: HFileKijiTap => Some(hfkt.tableUri)
        case _ => None
      }
    }

    val conf: Option[Configuration] = mode match {
      case Hdfs(_, c) => Some(c)
      case HadoopTest(c, _) => Some(c)
      case _ => None
    }

    val flowCounterMap: Map[String, Long] = flowCounters.map {
      triple: (String, String, Long) => {
        val (group, counter, count) = triple
        ("%s:%s".format(group, counter), count)
      }
    }.toMap

    val flowStepCountersIterable: Iterable[Map[String, Long]] = flowStepCounters.map {
      flowStepSet: Set[(String, String, Long)] => flowStepSet.map {
        triple: (String, String, Long) => {
          val (group, counter, count) = triple
          ("%s:%s".format(group, counter), count)
        }
      }.toMap
    }.toIterable

    val extendedInfo: Map[String, String] = args.list(KijiJob.extendedInfoArgsKey).map {
      s: String => {
        val splits: Array[String] = s.split(':')
        Preconditions.checkState(splits.size == 2, "Too many ':'s in argument: %s", s)
        (splits(0), splits(1))
      }
    }.toMap

    val instanceUris: Set[KijiURI] =
        (flowDef.getSources.asScala.toList ++ flowDef.getSinks.asScala)
        .map { pair: (String, Tap[_, _, _]) => maybeGetTableUri(pair._2) }
        .flatten
        .toSet
        .map { uriString: String => KijiURI.newBuilder(uriString).build() }

    instanceUris.foreach { uri: KijiURI =>
      val kiji: Kiji = if (conf.isDefined) {
        Kiji.Factory.open(uri, conf.get)
      } else {
        Kiji.Factory.open(uri)
      }
      try {
        recordJobHistory(
            startTime,
            endTime,
            jobSuccess,
            conf,
            kiji,
            flowCounterMap,
            extendedInfo,
            flowStepCountersIterable
        )
      } finally {
        kiji.release()
      }
    }
  }

  /**
   * Record the history of this job into the given Kiji instance.
   *
   * @param startTime in milliseconds since the epoch at which the job started.
   * @param endTime in milliseconds since the epoch at which the job ended.
   * @param jobSuccess whether the job completed successfully.
   * @param conf is the job configuration wrapped in [[Option]] object. Contains None if no
   *             configuration is passed.
   * @param kiji instance into which to record the job history.
   * @param counterMap to record in the job history.
   * @param extendedInfo to record in the job history.
   */
  private def recordJobHistory(
      startTime: Long,
      endTime: Long,
      jobSuccess: Boolean,
      conf: Option[Configuration],
      kiji: Kiji,
      counterMap: Map[String, Long],
      extendedInfo: Map[String, String],
      flowStepCountersIterable :Iterable[Map[String, Long]]
  ) {
    val expressJobHistoryTable: ExpressJobHistoryKijiTable = ExpressJobHistoryKijiTable(kiji)
    try {
      expressJobHistoryTable.recordJob(
          uniqueId.get,
          name,
          startTime,
          endTime,
          jobSuccess,
          conf,
          counterMap,
          extendedInfo,
          flowStepCountersIterable
      )
    } finally {
      expressJobHistoryTable.close()
    }
  }

  override def run: Boolean = {
    val startTime: Long = System.currentTimeMillis()
    val jobSuccess: Boolean = super.run
    val endTime: Long = System.currentTimeMillis()
    recordJobHistory(startTime, endTime, jobSuccess)

    // Output registered histograms
    histograms.foreach { histogram: HistogramConfig =>
      TupleProfiling.writeHistogram(histogram, flowCounters)
    }

    return jobSuccess
  }
}

/** Companion object to KijiJob. */
object KijiJob {
  /** Logger for the KijiJob class. */
  val logger: Logger = LoggerFactory.getLogger(classOf[KijiJob])

  /**
   * Specify a list of 'key:value' pairs to have them recorded into the Job History table entry for
   * this job. Keys may not contain ':'.
   */
  val extendedInfoArgsKey: String = "extendedInfo"

  /** Arg used by KijiExpress to determine whether to add the classpath to the dist. cache. */
  val addClasspathArg: String = "add-classpath-to-dcache"

  /** Arg used by KijiExpress, whose values are comma-separated jars to add to the dist.cache. */
  val tmpjarsArg: String = "tmpjars"

  /** Mapreduce configuration property for tmpjars. */
  val tmpjarsConfigProperty: String = "tmpjars"

  private[express] class CounterListener extends FlowListener with Serializable {

    private var _flowCounters: Set[(String, String, Long)] = Set()

    private var _flowStepCounters: Iterable[Set[(String, String, Long)]] = Iterable(Set())

    def flowCounters: Set[(String, String, Long)] = _flowCounters

    def flowStepCounters : Iterable[Set[(String, String, Long)]] =_flowStepCounters

    override def onStopping(flow: Flow[_]): Unit = {}

    override def onStarting(flow: Flow[_]): Unit = {}

    override def onThrowable(flow: Flow[_], throwable: Throwable): Boolean = false

    override def onCompleted(flow: Flow[_]): Unit = {

      _flowCounters = flow.getFlowStats.getCounterGroups.asScala.toSet.flatMap {
        group: String => flow.getFlowStats.getCountersFor(group).asScala.map {
          counter: String =>
            (group, counter, flow.getFlowStats.getCounterValue(group, counter))
        }
      }

      _flowStepCounters = flow.getFlowStats.getFlowStepStats.asScala.map {
        flowStepStats: FlowStepStats =>
          flowStepStats match {
            //In case this is a hadoop job, pull stats from cascading.
            case hadoopStepStats: HadoopStepStats =>
              val jobOption: Option[RunningJob] = Option(hadoopStepStats.getRunningJob)
              jobOption match {
                case Some(runningJob) => {
                  Option[Counters](runningJob.getCounters()) match {
                    case Some(counters) => counters
                        .getGroupNames
                        .asScala
                        .toSet
                        .flatMap { group: String =>
                          counters
                              .getGroup(group)
                              .iterator
                              .asScala
                              .map { counter: Counter =>
                                (group, counter.getName, counter.getValue)
                              }
                        }
                    // TODO EXP-479: Handle null counters better than returning the empty Set.
                    case None => {
                      logger.warn("Flow step counters from flow step stats {} were null, " +
                          "recording no counters.", flowStepStats)
                      Set[(String, String, Long)]()
                    }
                  }
                }
                case None => Set[(String, String, Long)]()
              }
            //In case this is a local job, only pull available stats.
            case localStepStats: LocalStepStats =>
              localStepStats.getCounterGroups.asScala.toSet.flatMap {
                group: String => localStepStats.getCountersFor(group).asScala.map {
                  counter: String =>
                    (group, counter, localStepStats.getCounterValue(group, counter))
                }
              }
          }
      }
    }
  }

  /**
   * @return The current jars on the classpath, formatted as file:///path/to/jar,file://path/to/jar2
   */
  def classpathJars(): Option[String] = {
    val classpath: String =
      sys.env.get("CLASSPATH") match {
        case Some(classpath) => classpath
        case None => sys.props.get("java.class.path").getOrElse {
          logger.warn(
            "Cannot find classpath jars using $CLASSPATH or system property java.class.path.")
          ""
        }
      }
    val hdfsFormattedClasspath: String = classpath
        .split(sys.props.get("path.separator").get)
        // TODO (EXP-493): Tar up directories so they can also go on the dist cache.
        .filter{ fileName: String => fileName.toLowerCase().endsWith(".jar") ||
            fileName.toLowerCase().endsWith(".zip")
        }
        .map{ fileName: String => "file://" + fileName }
        .mkString(",")
    if (hdfsFormattedClasspath.isEmpty) {
      None
    } else {
      Some(hdfsFormattedClasspath)
    }
  }
}

