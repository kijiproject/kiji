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

package org.kiji.rest.plugins;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;

import org.kiji.rest.GeneralExceptionMapper;
import org.kiji.rest.KijiClient;
import org.kiji.rest.KijiRESTConfiguration;
import org.kiji.rest.representations.KijiRestEntityId;
import org.kiji.rest.representations.SchemaOption;
import org.kiji.rest.resources.InstanceResource;
import org.kiji.rest.resources.InstancesResource;
import org.kiji.rest.resources.KijiRESTResource;
import org.kiji.rest.resources.RowsResource;
import org.kiji.rest.resources.TableResource;
import org.kiji.rest.resources.TablesResource;
import org.kiji.rest.serializers.AvroToJsonStringSerializer;
import org.kiji.rest.serializers.JsonToKijiRestEntityId;
import org.kiji.rest.serializers.JsonToSchemaOption;
import org.kiji.rest.serializers.KijiRestEntityIdToJson;
import org.kiji.rest.serializers.SchemaOptionToJson;
import org.kiji.rest.serializers.TableLayoutToJsonSerializer;
import org.kiji.rest.serializers.Utf8ToJsonSerializer;

/**
 * Installs default KijiREST endpoints into the Dropwizard environment.
 */
public class StandardKijiRestPlugin implements KijiRestPlugin {

  @Override
  public void install(
      final KijiClient kijiClient,
      final KijiRESTConfiguration configuration,
      final Environment environment) {
    // Adds custom serializers.
    registerSerializers(environment.getObjectMapperFactory());

    // Adds exception mappers to print better exception messages to the client than what
    // Dropwizard does by default.
    environment.addProvider(new GeneralExceptionMapper());

    // Adds resources.
    environment.addResource(new KijiRESTResource());
    environment.addResource(new InstancesResource(kijiClient));
    environment.addResource(new InstanceResource(kijiClient));
    environment.addResource(new TableResource(kijiClient));
    environment.addResource(new TablesResource(kijiClient));
    environment.addResource(new RowsResource(kijiClient,
        environment.getObjectMapperFactory().build(),
        configuration.getFresheningConfiguration()));
  }

  /**
   * Registers custom serializers with the Jackson ObjectMapper via DropWizard's
   * ObjectMapperFactory. This is used by both the service initialization and the test
   * setup method to ensure consistency between test and production.
   *
   * @param mapperFactory is the ObjectMapperFactory.
   */
  public static void registerSerializers(ObjectMapperFactory mapperFactory) {
    // TODO: Add a module to convert btw Avro's specific types and JSON. The default
    // mapping seems to throw an exception.
    SimpleModule module = new SimpleModule("KijiRestModule", new Version(1, 0, 0, null,
        "org.kiji.rest", "serializers"));
    module.addSerializer(new AvroToJsonStringSerializer());
    module.addSerializer(new Utf8ToJsonSerializer());
    module.addSerializer(new TableLayoutToJsonSerializer());
    module.addSerializer(new SchemaOptionToJson());
    module.addDeserializer(SchemaOption.class, new JsonToSchemaOption());
    module.addSerializer(new KijiRestEntityIdToJson());
    module.addDeserializer(KijiRestEntityId.class, new JsonToKijiRestEntityId());
    mapperFactory.registerModule(module);
  }

}
