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
package org.kiji.scoring.lib.produce;

import java.io.IOException;
import java.util.Map;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.io.RuntimeIOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.impl.InternalFreshenerContext;

/**
 * KijiProducer implementation which runs a ScoreFunction implementation by name.
 * <p>
 *   ScoreFunctionProducer can only run ScoreFunction implementations whose getDataRequest methods
 *   do not require the client data request, KeyValueStores, or for the ScoreFunction to have been
 *   set up ahead of time. Effectively getDataRequest may only use the attached column and
 *   parameters. These restrictions are imposed by the order of method calls of KijiProducers.
 * </p>
 * <p>
 *   To run ScoreFunctionProducer you must ensure that the jar containing your ScoreFunction
 *   implementation is available in the distributed cache of your mapreduce cluster. This can be
 *   done using the Java job builder API via
 *   {@link org.kiji.mapreduce.framework.MapReduceJobBuilder#addJarDirectory(String)} or on the
 *   command line via the --lib flag of the 'produce' CLI tool.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class ScoreFunctionProducer extends KijiProducer {

  public static final String SCORE_FUNCTION_PRODUCER_CONF_KEY =
      "org.kiji.scoring.lib.produce.ScoreFunctionProducer.conf";
  private static final Gson GSON = new Gson();

  /**
   * Container class deserialized from JSON representing the ScoreFunction to run in this producer.
   */
  private static final class ScoreFunctionConf {
    // CSOFF: MemberName
    private String scoreFunctionClass;
    private String attachedColumn;
    private Map<String, String> parameters;
    // CSON: MemberName
  }

  /**
   * FreshenerContext implementation which delegates to a KijiContext to provide KeyValueStores and
   * to an InternalFreshenerContext to provide all other methods.
   */
  private static final class ScoreFunctionProducerFreshenerContext implements FreshenerContext {

    private final KijiContext mGetStoresDelegate;
    private final InternalFreshenerContext mOtherDelegate;

    /**
     * Initialize a new ScoreFunctionProducerFreshenerContext with the given delegates.
     *
     * @param getStoresDelegate a KijiContext from which to get KeyValueStores.
     * @param otherDelegate an InternalFreshenerContext with which to provide all other methods.
     */
    private ScoreFunctionProducerFreshenerContext(
        final KijiContext getStoresDelegate,
        final InternalFreshenerContext otherDelegate
    ) {
      mGetStoresDelegate = getStoresDelegate;
      mOtherDelegate = otherDelegate;
    }

    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getClientRequest() {
      return mOtherDelegate.getClientRequest();
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> KeyValueStoreReader<K, V> getStore(final String storeName) throws IOException {
      return mGetStoresDelegate.getStore(storeName);
    }

    /** {@inheritDoc} */
    @Override
    public String getParameter(final String key) {
      return mOtherDelegate.getParameter(key);
    }

    /** {@inheritDoc} */
    @Override
    public Map<String, String> getParameters() {
      return mOtherDelegate.getParameters();
    }

    /** {@inheritDoc} */
    @Override
    public KijiColumnName getAttachedColumn() {
      return mOtherDelegate.getAttachedColumn();
    }
  }

  private ScoreFunctionConf mScoreFunctionConf;
  private ScoreFunction<?> mScoreFunction;
  private InternalFreshenerContext mInternalFreshenerContextDelegate;

  /**
   * Create a new instance of the given ScoreFunction implementation class.
   *
   * @param scoreFunctionClassName fully qualified name of the ScoreFunction class to instantiate.
   * @return a new instance of the given ScoreFunction implementation.
   */
  @SuppressWarnings("unchecked")
  private static ScoreFunction<?> scoreFunctionForName(
      final String scoreFunctionClassName
  ) {
    try {
      return (ScoreFunction<?>) ReflectionUtils.newInstance(
          Class.forName(scoreFunctionClassName).asSubclass(ScoreFunction.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void setConf(
      final Configuration conf
  ) {
    super.setConf(conf);
    mScoreFunctionConf = GSON.fromJson(
        getConf().get(SCORE_FUNCTION_PRODUCER_CONF_KEY), ScoreFunctionConf.class);
    mScoreFunction = scoreFunctionForName(mScoreFunctionConf.scoreFunctionClass);
    mInternalFreshenerContextDelegate = InternalFreshenerContext.create(
        new KijiColumnName(mScoreFunctionConf.attachedColumn), mScoreFunctionConf.parameters);
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return mScoreFunction.getRequiredStores(mInternalFreshenerContextDelegate);
  }

  /** {@inheritDoc} */
  @Override
  public void setup(
      final KijiContext context
  ) throws IOException {
    mScoreFunction.setup(
        new ScoreFunctionProducerFreshenerContext(context, mInternalFreshenerContextDelegate));
  }

  /**
   * {@inheritDoc}
   * <p>
   *   ScoreFunctionProducer can only run ScoreFunction implementations whose getDataRequest methods
   *   do not require the client data request, KeyValueStores, or for the ScoreFunction to have been
   *   set up ahead of time. Effectively getDataRequest may only use the attached column and
   *   parameters. These restrictions are imposed by the order of method calls of KijiProducers.
   * </p>
   */
  @Override
  public KijiDataRequest getDataRequest() {
    try {
      return mScoreFunction.getDataRequest(mInternalFreshenerContextDelegate);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return mScoreFunctionConf.attachedColumn;
  }

  /**
   * {@inheritDoc}
   * <p>
   *   Runs {@link ScoreFunction#score(org.kiji.schema.KijiRowData,
   *   org.kiji.scoring.FreshenerContext)} and writes the return value.
   * </p>
   */
  @Override
  public void produce(
      final KijiRowData input, final ProducerContext context
  ) throws IOException {
    ScoreFunction.TimestampedValue<?> scoringResult = mScoreFunction.score(
        input,
        new ScoreFunctionProducerFreshenerContext(context, mInternalFreshenerContextDelegate));
    context.put(scoringResult.getTimestamp(), scoringResult.getValue());
  }

  /** {@inheritDoc} */
  @Override
  public void cleanup(
      final KijiContext context
  ) throws IOException {
    mScoreFunction.cleanup(
        new ScoreFunctionProducerFreshenerContext(context, mInternalFreshenerContextDelegate));
  }
}
