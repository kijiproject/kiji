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
package org.kiji.scoring.lib.server;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.avro.Schema;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.FromJson;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerSetupContext;
import org.kiji.scoring.ScoreFunction;

/**
 * ScoreFunction implementation which delegates to the ScoringServer to produce a score.
 * <p>
 *   This ScoreFunction is in KijiScoring to simplify packaging and classpath management for users
 *   of the Kiji ScoringServer.
 * </p>
 *
 * <p>
 *   Operation of this ScoreFunction relies on the existence of a Kiji system table key-value pair
 *   containing the location of the ScoringServer which will perform remote scoring.
 *   <ul>
 *     <li>
 *       org.kiji.scoring.lib.server.ScoringServerScoreFunction.base_url_key stores the base URL of
 *       the scoring server. The system table value should be a UTF-8 encoded string.
 *     </li>
 *   </ul>
 * </p>
 *
 * <p>
 *   This ScoreFunction uses two parameter keys to configure its behavior.
 *   <ul>
 *     <li>
 *       org.kiji.scoring.lib.server.ScoringServerScoreFunction.instance_uri stores the KijiURI of
 *       the instance in which this ScoreFunction is running.
 *     </li>
 *     <li>
 *       org.kiji.scoring.lib.server.ScoringServerSCoreFunction.model_id_key stores the modelId of
 *       the model to use to generate a score.
 *     </li>
 *   </ul>
 *   The values of these parameters are read during setup and stored internally so that the behavior
 *   of this ScoreFunction cannot be modified by modifying the value of these keys with request time
 *   parameters. Both of these values are set by the kiji model-repo fresh-model tool when a Kiji
 *   model repository model is attached as a Freshener using remote scoring.
 * </p>
 */
@ApiAudience.Framework
@ApiStability.Experimental
public final class ScoringServerScoreFunction extends ScoreFunction<Object> {

  public static final Logger LOG = LoggerFactory.getLogger(ScoringServerScoreFunction.class);
  public static final String SCORING_SERVER_BASE_URL_SYSTEM_KEY =
      "org.kiji.scoring.lib.server.ScoringServerScoreFunction.base_url_key";
  public static final String SCORING_SERVER_MODEL_ID_PARAMETER_KEY =
      "org.kiji.scoring.lib.server.ScoringServerScoreFunction.model_id_key";
  public static final String SCORING_SERVER_INSTANCE_URI_PARAMETER_KEY =
      "org.kiji.scoring.lib.server.ScoringServerScoreFunction.instance_uri";
  private static final Gson GSON = new Gson();

  /** Container class for deserializing JSON server responses. */
  private static final class ScoringServerResponse {
    // CSOFF: MemberName - names do not match naming pattern so that GSON can write into them.
    private String family;
    private String qualifier;
    private long timestamp;
    private String value;
    private String schema;
    // CSON: MemberName
  }

  /**
   * Build the URL from which to retrieve a score by appending the necessary parameters to the base
   * model URL.
   *
   * @param modelBaseURL URL of the scoring servlet for this model.
   * @param eid the entity to score.
   * @param clientRequest client's data request which triggered the run of this ScoreFunction.
   * @param params an optional map of per-request parameters to be passed to the server.
   *
   * @return the URL from which to retrieve a score.
   * @throws MalformedURLException in case the URL cannot be created.
   */
  private static URL getScoringServerEndpoint(
      final String modelBaseURL,
      final EntityId eid,
      final KijiDataRequest clientRequest,
      final Map<String, String> params
  ) throws MalformedURLException {
    final String base64Request = Base64.encodeBase64URLSafeString(
        SerializationUtils.serialize(clientRequest));
    final StringBuilder urlStringBuilder;
    try {
      urlStringBuilder = new StringBuilder(
          String.format("%s?eid=%s&request=%s",
              modelBaseURL,
              URLEncoder.encode(eid.toShellString(), Charsets.UTF_8.name()),
              base64Request));
      for (Map.Entry<String, String> entry : params.entrySet()) {
          urlStringBuilder.append(String.format(
              "&fresh.%s=%s",
              URLEncoder.encode(entry.getKey(), "UTF-8"),
              URLEncoder.encode(entry.getValue(), "UTF-8")
          ));
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return new URL(urlStringBuilder.toString());
  }

  /**
   * Get the base URL for the scoring servlet for the given model from the given scoring server.
   *
   * @param scoringServerBaseURL URL of the scoring server from which to get the location of the
   *     model.
   * @param modelId fully qualified model name for which to get the scoring endpoint.
   * @return the base URL of the scoring servlet for the given model.
   * @throws IOException in case of an error getting the model URL from the scoring server.
   */
  @SuppressWarnings("unchecked")
  private static String getModelBaseURL(
      final String scoringServerBaseURL,
      final String modelId
  ) throws IOException {
    final URL getModelURL = new URL(scoringServerBaseURL + "/admin/get?model=" + modelId);
    final String response = IOUtils.toString(getModelURL.openStream());
    final Map<String, String> modelMap = GSON.fromJson(response, Map.class);
    return scoringServerBaseURL + "/" + modelMap.get(modelId);
  }

  /**
   * Get the ScoringServer base URL for the given Kiji instance.
   *
   * @param instanceUri KijiURI of the instance for which to get the ScoringServer base URL.
   * @return the ScoringServer base URL for the given Kiji instance.
   * @throws IOException in case of an error retrieving the base URL.
   */
  private static String getScoringServerBaseUrl(
      final KijiURI instanceUri
  ) throws IOException {
    final Kiji kiji = Kiji.Factory.open(instanceUri);
    try {
      final byte[] bytes = kiji.getSystemTable().getValue(SCORING_SERVER_BASE_URL_SYSTEM_KEY);
      if (null == bytes) {
        throw new RuntimeException("Cannot find ScoringServer base URL in system table.");
      } else {
        return Bytes.toString(bytes);
      }
    } finally {
      kiji.release();
    }
  }

  private String mModelBaseURL;

  /** {@inheritDoc} */
  @Override
  public void setup(
      final FreshenerSetupContext context
  ) throws IOException {
    final KijiURI uri = KijiURI.newBuilder(
        context.getParameter(SCORING_SERVER_INSTANCE_URI_PARAMETER_KEY)).build();
    mModelBaseURL = getModelBaseURL(
        getScoringServerBaseUrl(uri),
        context.getParameter(SCORING_SERVER_MODEL_ID_PARAMETER_KEY));
  }

  /**
   * {@inheritDoc}
   * <p>
   *   Because this ScoreFunction delegates to the ScoringServer to calculate scores, no data is
   *   required.
   * </p>
   */
  @Override
  public KijiDataRequest getDataRequest(final FreshenerContext context) throws IOException {
    return KijiDataRequest.empty();
  }

  /** {@inheritDoc} */
  @Override
  public TimestampedValue<Object> score(
      final KijiRowData dataToScore, final FreshenerContext context
  ) throws IOException {
    final URL scoringServerEndpoint = getScoringServerEndpoint(
        mModelBaseURL,
        dataToScore.getEntityId(),
        context.getClientRequest(),
        context.getParameters()
    );

    final String scoreJSON = IOUtils.toString(scoringServerEndpoint.openStream(), "UTF-8");

    try {
      final ScoringServerResponse response = GSON.fromJson(scoreJSON, ScoringServerResponse.class);
      final KijiColumnName responseColumn = new KijiColumnName(response.family, response.qualifier);
      if (context.getAttachedColumn().equals(responseColumn)) {
        return TimestampedValue.create(response.timestamp,
            FromJson.fromJsonString(response.value, new Schema.Parser().parse(response.schema)));
      } else {
        throw new IllegalStateException(String.format(
            "Column name found in response: %s does not match Freshener attached column: %s",
            responseColumn,
            context.getAttachedColumn()));
      }
    } catch (JsonSyntaxException jse) {
      throw new RuntimeException(jse);
    }
  }
}
