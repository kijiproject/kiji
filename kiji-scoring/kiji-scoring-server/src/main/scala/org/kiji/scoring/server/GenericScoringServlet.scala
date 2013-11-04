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


import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Preconditions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.ReflectionUtils

import org.kiji.express.avro.AvroModelDefinition
import org.kiji.express.avro.AvroModelEnvironment
import org.kiji.express.modeling.framework.ScoreProducer
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.withKiji
import org.kiji.express.util.Resources.withKijiTableReader
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.modelrepo.ArtifactName
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.ModelLifeCycle
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.tools.ToolUtils
import org.kiji.schema.util.ProtocolVersion
import org.kiji.schema.util.ToJson
import org.kiji.web.KijiWebContext

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

  // Set during init().
  var mInputKiji: Kiji = null
  var mInputTable: KijiTable = null
  var mScoreProducer: KijiProducer = null
  var mKVStoreFactory: KeyValueStoreReaderFactory = null
  var mOutputColumn: KijiColumnName = null

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

        val avroDefinition: AvroModelDefinition = modelLifeCycle.getDefinition
        val avroEnvironment: AvroModelEnvironment = modelLifeCycle.getEnvironment
        // TODO: Is is alright that I am getting the input spec directly through the Avro
        // record as opposed to through the KijiExpress wrapper classes?
        val inputURIString: String =
            avroEnvironment.getScoreEnvironment.getInputSpec.getKijiSpecification.getTableUri
        val inputURI: KijiURI = KijiURI.newBuilder(inputURIString).build()
        mInputKiji = Kiji.Factory.open(inputURI)
        mInputTable = mInputKiji.openTable(inputURI.getTable)

        val conf: Configuration = new Configuration(false)
        conf.set(ScoreProducer.modelDefinitionConfKey, ToJson.toJsonString(avroDefinition))
        conf.set(ScoreProducer.modelEnvironmentConfKey, ToJson.toJsonString(avroEnvironment))
        mScoreProducer = ReflectionUtils.newInstance(classOf[ScoreProducer], conf)

        mKVStoreFactory = KeyValueStoreReaderFactory.create(mScoreProducer.getRequiredStores)
        mOutputColumn = new KijiColumnName(mScoreProducer.getOutputColumn)

        mScoreProducer.setup(new KijiWebContext(mKVStoreFactory, mOutputColumn))
      }
    }
  }

  override def destroy() {
    mKVStoreFactory.close()
    mScoreProducer.cleanup(new KijiWebContext(mKVStoreFactory, mOutputColumn))
    mInputTable.release()
    mInputKiji.release()
  }

  override def doGet(
    req: HttpServletRequest,
    resp: HttpServletResponse
  ) {
    // Fetch the entity_id parameter from the URL. Fail if not specified.
    val eidString: String =
        Preconditions.checkNotNull(req.getParameter("eid"), "Entity ID required!", "");
    // TODO also fetch request parameters and pass them to the ScoreProducer.

    doAndClose(new OutputStreamWriter(resp.getOutputStream, "UTF-8")) {
      osw: OutputStreamWriter => {
        doAndClose(new BufferedWriter(osw)) {
          bw: BufferedWriter => {
            val entityId: EntityId =
                ToolUtils.createEntityIdFromUserInputs(eidString, mInputTable.getLayout);
            val dataRequest: KijiDataRequest = mScoreProducer.getDataRequest;
            // TODO replace this with a reader pool.
            withKijiTableReader(mInputTable) {
              reader: KijiTableReader => {
                val rowData: KijiRowData = reader.get(entityId, dataRequest);
                val context: KijiWebContext =
                    new KijiWebContext(mKVStoreFactory, mOutputColumn)
                mScoreProducer.produce(rowData, context);
                val mapper: ObjectMapper = new ObjectMapper();
                bw.write(mapper.valueToTree(context.getWrittenCell).toString);
              }
            }
          }
        }
      }
    }
  }
}
