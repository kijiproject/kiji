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
package org.kiji.scoring;

import java.util.Map;

import com.google.common.collect.Maps;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.scoring.avro.ParameterDescription;
import org.kiji.scoring.avro.ParameterScope;
import org.kiji.scoring.params.ParamParser;
import org.kiji.scoring.params.ParamSpec;

/**
 * A group of parameters, used at setup or scoring time for a ParameterProvider (e.g ScoreFunction
 * or KijiFreshnessPolicy). Each ParameterProvider should have corresponding Parameters
 * implementations for setup and scoring, and annotate their fields using Param annotations. These
 * Params are then used for documentation, and also to determine how to parse the contexts for
 * parameter values. The Parameters implementations are typically an inner class of the
 * ParameterProvider, but this isn't required.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
public class Parameters {

  /**
   * Simple empty Parameters, to be used as a return value instead of null.
   */
  private static final Parameters EMPTY_PARAMETERS = new Parameters();

  /**
   * Returns an empty group of parameters.
   * @return an empty group of parameters.
   */
  public static final Parameters getEmptyParameters() {
    return EMPTY_PARAMETERS;
  }

  /**
   * Get the description of all parameters from a ParameterProvider.
   * @param provider the ParameterProvider from which to get the parameter descriptions.
   * @return the list of descriptions.
   */
  public static Map<String, ParameterDescription> getDescriptions(
      final ParameterProvider provider
  ) {
    final Parameters setupParams = provider.getSetupParameters();
    final Parameters runtimeParams = provider.getRuntimeParameters();

    final Map<String, ParameterDescription> descriptions = Maps.newHashMap();

    // Get the setup parameters
    descriptions.putAll(setupParams.getDescriptions(ParameterScope.SETUP));
    descriptions.putAll(runtimeParams.getDescriptions(ParameterScope.SCORING));

    return descriptions;
  }

  /**
   * Fills in all member variables. This can be used to initialize both setup and runtime
   * Parameters. Repeatedly calling parse() is not recommended, as it will overwrite old values
   * with the value from the most recent context.
   *
   * @param context the Freshener context
   */
  public final void parse(
      final FreshenerGetStoresContext context
  ) {
    final Map<String, ParamSpec> paramMap = ParamParser.extractParamDeclarations(this);

    // For each param, get the parameter from the context.
    try {
      for (Map.Entry<String, ParamSpec> entry : paramMap.entrySet()) {
        final String paramName = entry.getKey();
        final String paramValue = context.getParameter(paramName);

        final ParamSpec paramSpec = entry.getValue();
        paramSpec.setValue(paramValue);
      }
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    }
  }

    /**
   * Get the description of parameters in the Parameters.
   * @param paramScope whether or not the parameters are runtime parameters.
   * @return the list of descriptions.
   */
  public Map<String, ParameterDescription> getDescriptions(
      ParameterScope paramScope
  ) {
    final Map<String, ParameterDescription> descriptions = Maps.newHashMap();
    final Map<String, ParamSpec> paramMap = ParamParser.extractParamDeclarations(this);

    // For each param, create a ParameterDescription record.
    for (Map.Entry<String, ParamSpec> entry : paramMap.entrySet()) {
      final ParamSpec paramSpec = entry.getValue();
      final ParameterDescription.Builder builder = ParameterDescription.newBuilder()
          .setDescription(paramSpec.getDescription())
          .setType(paramSpec.getTypeName())
          .setRequired(paramSpec.isRequired())
          .setScope(paramScope);

      if (!paramSpec.isRequired()) {
        builder.setDefaultValue(paramSpec.getDefaultValue());
      }

      descriptions.put(paramSpec.getName(), builder.build());
    }

    return descriptions;
  }


}
