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

package org.kiji.scoring.server

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.enumerationAsScalaIteratorConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Preconditions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants

import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.flow.ColumnRequestInput
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.util.GenericRowDataConverter
import org.kiji.express.util.Tuples
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.withKiji
import org.kiji.express.util.Resources.withKijiTableReader
import org.kiji.modeling.ExtractFn
import org.kiji.modeling.Extractor
import org.kiji.modeling.KeyValueStore
import org.kiji.modeling.ScoreFn
import org.kiji.modeling.Scorer
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.framework.ModelConverters
import org.kiji.modeling.impl.ModelJobUtils
import org.kiji.modeling.impl.ModelJobUtils.PhaseType.SCORE
import org.kiji.modelrepo.ArtifactName
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.ModelLifeCycle
import org.kiji.schema.{ EntityId => JEntityId }
import org.kiji.schema.Kiji
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.tools.ToolUtils
import org.kiji.schema.util.ProtocolVersion
import org.kiji.scoring.server.kvstore.lib.ThreadLocalMapKVStore
import org.kiji.web.KijiScoringServerCell

/**
 * Servlet implementation that executes the scoring phase of a model lifecycle deployed
 * from the model repository. The environment and definition configuration is used to construct
 * the appropriate phase implementation objects and the producer wrapping this performs the
 * actual execution.
 */
class GenericScoringServlet extends HttpServlet {

  val MODEL_REPO_URI_KEY: String = "model-repo-uri"
  val MODEL_GROUP_KEY: String  = "model-group"
  val MODEL_ARTIFACT_KEY: String  = "model-artifact"
  val MODEL_VERSION_KEY: String  = "model-version"
  // Name of the KVStore to use for per-request parameters
  val FRESHENER_PARAMETERS_KVSTORE: String = "freshener_parameters"

  val mapper: ObjectMapper = new ObjectMapper()

  // Set during init().
  var mInputKiji: Kiji = null
  var mInputTable: KijiTable = null
  var mDataRequest: KijiDataRequest = null
  var mModelDef: ModelDefinition = null
  var mModelEnv: ModelEnvironment = null
  var mOutputColumn: KijiColumnName = null
  var mExtractor: Option[Extractor] = None
  var mScorer: Scorer = null
  var mRowConverter: GenericRowDataConverter = null
  val mThreadLocalKVStore: ThreadLocalMapKVStore[String, String] =
      new ThreadLocalMapKVStore[String, String]

  override def init() {
    val modelName: String = getServletConfig.getInitParameter(MODEL_GROUP_KEY)
    val modelArtifact: String = getServletConfig.getInitParameter(MODEL_ARTIFACT_KEY)
    val modelVersion: ProtocolVersion =
        ProtocolVersion.parse(getServletConfig.getInitParameter(MODEL_VERSION_KEY))
    val artifactName: ArtifactName =
        new ArtifactName("%s.%s".format(modelName, modelArtifact), modelVersion)

    val modelRepoURIString: String = getServletConfig.getInitParameter(MODEL_REPO_URI_KEY)
    val modelRepoURI: KijiURI = KijiURI.newBuilder(modelRepoURIString).build()

    withKiji(modelRepoURI, HBaseConfiguration.create()) {
      kiji: Kiji => {
        val modelLifeCycle: ModelLifeCycle = doAndClose(KijiModelRepository.open(kiji)) {
          modelRepo: KijiModelRepository => modelRepo.getModelLifeCycle(artifactName)
        }

        mModelDef = ModelConverters.modelDefinitionFromAvro(modelLifeCycle.getDefinition)
        mModelEnv = ModelConverters.modelEnvironmentFromAvro(modelLifeCycle.getEnvironment)
        val inputURIString: String = mModelEnv.scoreEnvironment.get.inputSpec.tableUri
        val inputURI: KijiURI = KijiURI.newBuilder(inputURIString).build()
        mInputKiji = Kiji.Factory.open(inputURI)
        mInputTable = mInputKiji.openTable(inputURI.getTable)

        mDataRequest = ModelJobUtils.getDataRequest(mModelEnv, SCORE).get
        mOutputColumn = new KijiColumnName(ModelJobUtils.getOutputColumn(mModelEnv))
        mExtractor = mModelDef.scoreExtractorClass.map { _.newInstance() }
        mScorer = mModelDef.scorerClass.get.newInstance()
        mRowConverter = new GenericRowDataConverter(inputURI, new Configuration)
        mScorer.keyValueStores =
          Map(FRESHENER_PARAMETERS_KVSTORE -> KeyValueStore(mThreadLocalKVStore.open()))
      }
    }
  }

  override def destroy() {
    mInputTable.release()
    mInputKiji.release()
  }

  /**
   * Helper function to compute a score given a row data.
   *
   * @param input The KijiRowData to pass to the Score function.
   */
  private def score(input: KijiRowData): KijiScoringServerCell = {
    // TODO(EXP-283): Generalize this functionality inside kiji-modeling and utilize that instead.
    val ScoreFn(scoreFields, score) = mScorer.scoreFn

    // Setup fields.
    val fieldMapping: Map[String, KijiColumnName] = mModelEnv
      .scoreEnvironment
      .get
      .inputSpec
      .asInstanceOf[KijiInputSpec]
      .columnsToFields
      .toList
      // List of (ColumnRequestInput, Symbol) pairs
      .map { case (column: ColumnRequestInput, field: Symbol) => {
      (field.name, column.columnName)
    }}
      .toMap

    // Configure the row data input to decode its data generically
    val row = mRowConverter(input)

    // Prepare input to the extract phase.
    def getSlices(inputFields: Seq[String]): Seq[Any] = inputFields
        .map { (field: String) =>
          if (field == KijiScheme.entityIdField) {
            EntityId.fromJavaEntityId(row.getEntityId())
          } else {
            val columnName: KijiColumnName = fieldMapping(field.toString)

          // TODO(EXP-283/EXP-208): This should use KijiScheme#rowToTuple.
          // Build a slice from each column within the row.
          if (columnName.isFullyQualified) {
            val slice = row
                .iterator(columnName.getFamily, columnName.getQualifier)
                .asScala
                .toIterable
                .map { kijiCell: KijiCell[_] => Cell(kijiCell) }

            slice
          } else {
            val slice = row
                .iterator(columnName.getFamily)
                .asScala
                .toIterable
                .map { kijiCell: KijiCell[_] => Cell(kijiCell) }

            slice
          }
        }
      }

    val extractFnOption: Option[ExtractFn[_, _]] = mExtractor.map { _.extractFn }
    val scoreInput = extractFnOption match {
      // If there is an extractor, use its extractFn to set up the correct input and output fields
      case Some(ExtractFn(extractFields, extract)) => {
        val extractInputFields: Seq[String] = {
          // If the field specified is the wildcard field, use all columns referenced in this model
          // environment's field bindings.
          if (extractFields._1.isAll) {
            fieldMapping.keys.toSeq
          } else {
            Tuples.fieldsToSeq(extractFields._1)
          }
        }
        val extractOutputFields: Seq[String] = {
          // If the field specified in the results field, use all input fields from the extract
          // phase.
          if (extractFields._2.isResults) {
            extractInputFields
          } else {
            Tuples.fieldsToSeq(extractFields._2)
          }
        }

        val scoreInputFields: Seq[String] = {
          // If the field specified is the wildcard field, use all fields output by the extract
          // phase.
          if (scoreFields.isAll) {
            extractOutputFields
          } else {
            Tuples.fieldsToSeq(scoreFields)
          }
        }

        // Prepare input to the extract phase.
        val slices = getSlices(extractInputFields)

        // Get output from the extract phase.
        val featureVector: Product = Tuples.fnResultToTuple(
          extract(Tuples.tupleToFnArg(Tuples.seqToTuple(slices))))
        val featureMapping: Map[String, Any] = extractOutputFields
          .zip(featureVector.productIterator.toIterable)
          .toMap

        // Get a score from the score phase.
        val scoreInput: Seq[Any] = scoreInputFields.map { field => featureMapping(field) }

        scoreInput
      }
      // If there's no extractor, use default input and output fields
      case None => {
        val scoreInputFields = Tuples.fieldsToSeq(scoreFields)
        val inputOutputFields = fieldMapping.keys.toSeq
        val slices = getSlices(inputOutputFields)

        // Get output from the extract phase.
        val featureVector: Product = Tuples.seqToTuple(slices)
        val featureMapping: Map[String, Any] = inputOutputFields
          .zip(featureVector.productIterator.toIterable)
          .toMap

        // Get a score from the score phase.
        val scoreInput: Seq[Any] = scoreInputFields
          .map { field => featureMapping(field) }

        scoreInput
      }
    }

    // Return the calculated score.
    new KijiScoringServerCell(
        mOutputColumn.getFamily,
        mOutputColumn.getQualifier,
        HConstants.LATEST_TIMESTAMP,
        score(Tuples.tupleToFnArg(Tuples.seqToTuple(scoreInput)))
    );
  }


  override def doGet(
    req: HttpServletRequest,
    resp: HttpServletResponse
  ) {
    // Fetch the entity_id parameter from the URL. Fail if not specified.
    val eidString: String =
        Preconditions.checkNotNull(req.getParameter("eid"), "Entity ID required!", "");

    // Fetch a map of request parameters.
    val freshenerParameters = req
        .getParameterNames
        .asScala
        .collect {
          case x: String if x.startsWith("fresh.") => {
            (x.stripPrefix("fresh."), req.getParameter(x))
          }
        }
        .toMap
        .asJava

    if (!freshenerParameters.isEmpty) {
      mThreadLocalKVStore.registerThreadLocalMap(freshenerParameters)
    }

    doAndClose(new OutputStreamWriter(resp.getOutputStream, "UTF-8")) {
      osw: OutputStreamWriter => {
        doAndClose(new BufferedWriter(osw)) {
          bw: BufferedWriter => {
            val entityId: JEntityId =
                ToolUtils.createEntityIdFromUserInputs(eidString, mInputTable.getLayout)
            // TODO replace this with a reader pool.
            withKijiTableReader(mInputTable) {
              reader: KijiTableReader => {
                val rowData: KijiRowData = reader.get(entityId, mDataRequest)
                bw.write(mapper.valueToTree(score(rowData)).toString)
              }
            }
          }
        }
      }
    }
    mThreadLocalKVStore.unregisterThreadLocalMap
  }
}
