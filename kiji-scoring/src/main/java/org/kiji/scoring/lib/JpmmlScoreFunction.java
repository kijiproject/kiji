/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.scoring.lib;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.xml.bind.JAXBException;
import javax.xml.transform.Source;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.dmg.pmml.*;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalTime;
import org.joda.time.Seconds;
import org.jpmml.evaluator.*;
import org.jpmml.manager.PMMLManager;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.FreshenerSetupContext;
import org.kiji.scoring.ScoreFunction;

/**
 * A generic score function for scoring already-trained PMML-compliant (and supported by Jpmml)
 * models.
 *
 * Expected parameters:
 * <ul>
 *   <li>
 *     "org.kiji.scoring.lib.JpmmlScoreFunction.model-file" - The path to the trained model file.
 *   </li>
 *   <li>
 *     "org.kiji.scoring.lib.JpmmlScoreFunction.model-name" - The name of the model to load.
 *   </li>
 *   <li>
 *     "org.kiji.scoring.lib.JpmmlScoreFunction.predictor-column" - The name of the column
 *     containing the model's predictors.
 *   </li>
 *   <li>
 *     "org.kiji.scoring.lib.JpmmlScoreFunction.result-record-name" - The name of the record that
 *     will contain the predicted and output records.
 *   </li>
 * </ul>
 *
 * Note: Extensions (http://www.dmg.org/v4-2/GeneralStructure.html#xsdElement_Extension) are
 *     currently <b>NOT SUPPORTED</b>.
 */
@ApiAudience.Public
@ApiStability.Experimental
public final class JpmmlScoreFunction extends ScoreFunction<GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(JpmmlScoreFunction.class);

  /** Parameter name for specifying the path to the trained model file. */
  public static final String MODEL_FILE_PARAMETER =
      "org.kiji.scoring.lib.JpmmlScoreFunction.model-file";
  /** Parameter name for specifying the name of the trained model. */
  public static final String MODEL_NAME_PARAMETER =
      "org.kiji.scoring.lib.JpmmlScoreFunction.model-name";
  /** Parameter name for specifying the name of the predictor column. */
  public static final String PREDICTOR_COLUMN_PARAMETER =
      "org.kiji.scoring.lib.JpmmlScoreFunction.predictor-column";
  /** Parameter name for specifying the name of the result record. */
  public static final String RESULT_RECORD_PARAMETER =
      "org.kiji.scoring.lib.JpmmlScoreFunction.result-record-name";

  // For converting times between Avro and JPMML formats.
  public static final LocalTime MIDNIGHT = new LocalTime(0, 0, 0);
  public static final LocalDate DATE_YEAR_0 = new LocalDate(0, 1, 1);
  public static final LocalDateTime TIME_YEAR_0 = DATE_YEAR_0.toLocalDateTime(MIDNIGHT);
  public static final LocalDate DATE_YEAR_1960 = new LocalDate(1960, 1, 1);
  public static final LocalDateTime TIME_YEAR_1960 = DATE_YEAR_1960.toLocalDateTime(MIDNIGHT);
  public static final LocalDate DATE_YEAR_1970 = new LocalDate(1970, 1, 1);
  public static final LocalDateTime TIME_YEAR_1970 = DATE_YEAR_1970.toLocalDateTime(MIDNIGHT);
  public static final LocalDate DATE_YEAR_1980 = new LocalDate(1980, 1, 1);
  public static final LocalDateTime TIME_YEAR_1980 = DATE_YEAR_1980.toLocalDateTime(MIDNIGHT);

  /** Stores the evaluator for the provided model. */
  private Evaluator mEvaluator = null;

  /** Stores the schema for the scores calculated by the provided model. */
  private Schema mResultSchema = null;

  /** Stores a mapping from PMML fields (data fields and output fields) to their PMML data types. */
  private Map<FieldName, DataType> mFieldNamesToPmmlDataTypes;

  /**
   * Checks to ensure required parameters have been provided, loads the Jpmml evaluator, and creates
   * the result record schema.
   *
   * {@inheritDoc}
   */
  @Override
  public void setup(
      final FreshenerSetupContext context
  ) throws IOException {
    super.setup(context);

    final Configuration configuration = HBaseConfiguration.create();

    // Ensure all parameters are specified correctly.
    final Map<String, String> parameters = context.getParameters();
    Preconditions.checkArgument(
        parameters.containsKey(MODEL_FILE_PARAMETER),
        String.format("Missing required parameter: %s", MODEL_FILE_PARAMETER)
    );
    Preconditions.checkArgument(
        parameters.containsKey(MODEL_NAME_PARAMETER),
        String.format("Missing required parameter: %s", MODEL_NAME_PARAMETER)
    );
    Preconditions.checkArgument(
        parameters.containsKey(PREDICTOR_COLUMN_PARAMETER),
        String.format("Missing required parameter: %s", PREDICTOR_COLUMN_PARAMETER)
    );
    Preconditions.checkArgument(
        parameters.containsKey(RESULT_RECORD_PARAMETER),
        String.format("Missing required parameter: %s", RESULT_RECORD_PARAMETER)
    );

    // Parse parameters.
    final Path modelFilePath = new Path(parameters.get(MODEL_FILE_PARAMETER));
    final String modelName = parameters.get(MODEL_NAME_PARAMETER);
    final String resultRecordName = parameters.get(RESULT_RECORD_PARAMETER);

    // Load the PMML model.
    final PMMLManager pmmlManager;
    try {
      final FileSystem fileSystem = modelFilePath.getFileSystem(configuration);
      try {
        final FSDataInputStream fsDataInputStream = fileSystem.open(modelFilePath);
        try {
          Source source = ImportFilter.apply(new InputSource(fsDataInputStream));
          final PMML pmml = JAXBUtil.unmarshalPMML(source);
          pmmlManager = new PMMLManager(pmml);
        } finally {
          fsDataInputStream.close();
        }
      } finally {
        fileSystem.close();
      }
    } catch (JAXBException e) {
      throw new IOException(e);
    } catch (SAXException e) {
      throw new IOException(e);
    }

    // Sanity check that the user-specified model exists in the PMML file.
    List<Model> modelList = pmmlManager.getModels();

    HashSet<String> modelNames = new HashSet<String>();
    for (Model model : modelList) {
      modelNames.add(model.getModelName());

    }
    if (!modelNames.contains(modelName)) {
      throw new KijiIOException(String.format(
          "Specified model '%s' does not exist in PMML file.  Model names are: %s.",
          modelName,
          modelNames
      ));
    }

    // Load the user-specified model.
    mEvaluator = (Evaluator) pmmlManager.getModelManager(
        modelName,
        ModelEvaluatorFactory.getInstance()
    );

    // Create the map of PMML field names to their PMML data types.
    mFieldNamesToPmmlDataTypes = new HashMap<FieldName, DataType>();

    // TODO (SCORE-174): Need a check that the names don't have dots in them.

    // Create a map from PMML field names to their Avro types.
    final DataDictionary dataDictionary = pmmlManager.getDataDictionary();
    for (DataField dataField : dataDictionary.getDataFields()) {
      Preconditions.checkNotNull(dataField.getDataType());
      mFieldNamesToPmmlDataTypes.put(dataField.getName(), dataField.getDataType());
    }

    final List<FieldName> resultFields = Lists.newArrayList();
    resultFields.addAll(mEvaluator.getTargetFields());

    // All of the target fields should already be in the data-type dictionary.
    for (FieldName fieldName : resultFields) {
      Preconditions.checkArgument(mFieldNamesToPmmlDataTypes.containsKey(fieldName));
    }

    // TODO (SCORE-172): Handle output fields correctly (they may not have data types).
    // Add data types for all of the output fields.
    for (FieldName outputFieldName : mEvaluator.getOutputFields()) {
      // See if we can look up the type of this field.
      OutputField outputField = mEvaluator.getOutputField(outputFieldName);
      Preconditions.checkNotNull(outputField);
      DataType dataType = outputField.getDataType();

      // TODO: Decide whether to allow models with duplicate output fields.
      // (Some of the JPMML-Rattle unit tests actually have this (!).
      //if (resultFields.contains(outputFieldName)) { continue; }
      Preconditions.checkArgument(!resultFields.contains(outputFieldName));

      resultFields.add(outputFieldName);
      Preconditions.checkArgument(!mFieldNamesToPmmlDataTypes.containsKey(outputFieldName));

      // Hopefully, most output fields in the PMML will have an explicit data type.
      if (dataType != null) {
        mFieldNamesToPmmlDataTypes.put(outputFieldName, dataType);
        continue;
      }

      // Try to guess the data type - probability is always a double.
      if (outputFieldName.getValue().startsWith("Probability_")) {
        mFieldNamesToPmmlDataTypes.put(outputFieldName, DataType.DOUBLE);
        continue;
      }

      // Try to guess the data type - "Predicted_foo" will always have the same type as "foo."
      if (outputFieldName.getValue().startsWith("Predicted_")) {
        final FieldName rootName = new FieldName(outputFieldName.getValue().substring(10));
        Preconditions.checkArgument(mFieldNamesToPmmlDataTypes.containsKey(rootName));
        mFieldNamesToPmmlDataTypes.put(
            outputFieldName,
            mFieldNamesToPmmlDataTypes.get(rootName)
        );
        continue;
      }

      // Fall back to string.
      LOG.warn(String.format(
          "Could not find data type in model for field '%s', falling back to STRING.",
          outputFieldName));
      mFieldNamesToPmmlDataTypes.put(outputFieldName, DataType.STRING);
    }

    mResultSchema = fieldNamesToSchema(resultRecordName, resultFields, mFieldNamesToPmmlDataTypes);
  }

  /**
   * Requests the provided predictor column.
   *
   * {@inheritDoc}
   */
  @Override
  public KijiDataRequest getDataRequest(
      final FreshenerContext context
  ) throws IOException {
    final KijiColumnName predictorColumnName =
        new KijiColumnName(context.getParameter(PREDICTOR_COLUMN_PARAMETER));
    return KijiDataRequest.create(
        predictorColumnName.getFamily(),
        predictorColumnName.getQualifier()
    );
  }

  /**
   * Generates a score using Jpmml's evaluators.
   *
   * {@inheritDoc}
   */
  @Override
  public TimestampedValue<GenericRecord> score(
      final KijiRowData dataToScore,
      final FreshenerContext context
  ) throws IOException {
    // Load appropriate arguments.
    final KijiColumnName predictorColumnName =
        new KijiColumnName(context.getParameter(PREDICTOR_COLUMN_PARAMETER));

    final GenericRecord predictors = dataToScore.getMostRecentValue(
        predictorColumnName.getFamily(),
        predictorColumnName.getQualifier()
    );

    // Build the arguments to the pmml model evaluator.
    final Map<FieldName, Object> arguments = Maps.newHashMap();
    for (FieldName field : mEvaluator.getActiveFields()) {
      final Object avroField;
      try {
        avroField = predictors.get(field.getValue());
      } catch (NullPointerException e) {
        throw new KijiIOException(String.format(
            "Trouble finding field '%s' in input data for scoring.",
            field));
      }
      final DataType dataType = mFieldNamesToPmmlDataTypes.get(field);
      Preconditions.checkNotNull(dataType);

      final Object convertedArgument = convertAvroToFieldValue(avroField, dataType);
      final Object preparedArgument;
      try {
        preparedArgument = mEvaluator.prepare(field, convertedArgument);
      } catch (Exception e) {
        throw new KijiIOException(String.format(
            "Trouble setting field '%s' to value '%s' in JPMML evaluator.",
            field,
            convertedArgument
        ));
      }
      arguments.put(field, preparedArgument);
    }

    // Calculate the scores.
    final Map<FieldName, ?> results = mEvaluator.evaluate(arguments);

    // Pack this into a record and write it to the column.
    final GenericRecordBuilder resultRecordBuilder = new GenericRecordBuilder(mResultSchema);
    for (Map.Entry<FieldName, ?> entry : results.entrySet()) {
      final FieldName fieldName = entry.getKey();

      // This actually happens sometimes!
      Preconditions.checkNotNull(fieldName);

      final Object jpmmlField = entry.getValue();

      // Look in the output fields and in the data fields for the data type.
      final DataType dataType = mFieldNamesToPmmlDataTypes.get(fieldName);
      Preconditions.checkNotNull(dataType);
      final Object convertedField = convertFieldValueToAvro(jpmmlField, dataType);
      resultRecordBuilder.set(entry.getKey().getValue(), convertedField);
    }
    return TimestampedValue.<GenericRecord>create(resultRecordBuilder.build());
  }

  /**
   * Converts a PMML data type into an Avro schema.
   *
   * @param dataType to convert.
   * @return an appropriate Avro schema.
   */
  public static Schema convertDataTypeToSchema(
      final DataType dataType
  ) {
    switch (dataType) {
      case STRING:
        return Schema.create(Schema.Type.STRING);
      case INTEGER:
        // PMML has no "long" type.
        return Schema.create(Schema.Type.LONG);
      case FLOAT:
        return Schema.create(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.create(Schema.Type.DOUBLE);
      case BOOLEAN:
        return Schema.create(Schema.Type.BOOLEAN);
      case DATE:
      case TIME:
      case DATE_TIME:
        return Schema.create(Schema.Type.STRING);
      case DATE_DAYS_SINCE_0:
      case DATE_DAYS_SINCE_1960:
      case DATE_DAYS_SINCE_1970:
      case DATE_DAYS_SINCE_1980:
      case TIME_SECONDS:
      case DATE_TIME_SECONDS_SINCE_0:
      case DATE_TIME_SECONDS_SINCE_1960:
      case DATE_TIME_SECONDS_SINCE_1970:
      case DATE_TIME_SECONDS_SINCE_1980:
        // Joda time returns these as integers.
        return Schema.create(Schema.Type.INT);
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported DataType: %s", dataType.value())
        );
    }
  }

  /**
   * Converts Avro data into its corresponding JPMML type.
   *
   * @param avroData to convert.
   * @param dataType of the data to convert.
   * @return a converted JPMML compatible datum.
   */
  public static Object convertAvroToFieldValue(
      final Object avroData,
      final DataType dataType
  ) {
    switch (dataType) {
      case STRING:
      case INTEGER:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return avroData;
      case DATE:
        final String dateString = (String) avroData;
        return LocalDate.parse(dateString);
      case TIME:
        final String timeString = (String) avroData;
        return LocalTime.parse(timeString);
      case DATE_TIME:
        final String dateTimeString = (String) avroData;
        return LocalDateTime.parse(dateTimeString);
      // Not sure if this datatype is supported by JPMML.
      case DATE_DAYS_SINCE_0:
        final Integer daysSinceYear0 = (Integer) avroData;
        return new DaysSinceDate(DATE_YEAR_0, DATE_YEAR_0.plusDays(daysSinceYear0));
      case DATE_DAYS_SINCE_1960:
        final Integer daysSinceYear1960 = (Integer) avroData;
        return new DaysSinceDate(DATE_YEAR_1960, DATE_YEAR_1960.plusDays(daysSinceYear1960));
      case DATE_DAYS_SINCE_1970:
        final Integer daysSinceYear1970 = (Integer) avroData;
        return new DaysSinceDate(DATE_YEAR_1970, DATE_YEAR_1970.plusDays(daysSinceYear1970));
      case DATE_DAYS_SINCE_1980:
        final Integer daysSinceYear1980 = (Integer) avroData;
        return new DaysSinceDate(DATE_YEAR_1980, DATE_YEAR_1980.plusDays(daysSinceYear1980));
      case TIME_SECONDS:
        final Integer secondsSinceMidnight = (Integer) avroData;
        return new SecondsSinceMidnight(Seconds.seconds(secondsSinceMidnight));
      // Not sure if this datatype is supported by JPMML.
      case DATE_TIME_SECONDS_SINCE_0:
        final Integer secondsSinceYear0 = (Integer) avroData;
        return new SecondsSinceDate(
            DATE_YEAR_0,
            TIME_YEAR_0.plusSeconds(secondsSinceYear0)
        );
      case DATE_TIME_SECONDS_SINCE_1960:
        final Integer secondsSinceYear1960 = (Integer) avroData;
        return new SecondsSinceDate(
            DATE_YEAR_1960,
            TIME_YEAR_1960.plusSeconds(secondsSinceYear1960)
        );
      case DATE_TIME_SECONDS_SINCE_1970:
        final Integer secondsSinceYear1970 = (Integer) avroData;
        return new SecondsSinceDate(
            DATE_YEAR_1970,
            TIME_YEAR_1970.plusSeconds(secondsSinceYear1970)
        );
      case DATE_TIME_SECONDS_SINCE_1980:
        final Integer secondsSinceYear1980 = (Integer) avroData;
        return new SecondsSinceDate(
            DATE_YEAR_1980,
            TIME_YEAR_1980.plusSeconds(secondsSinceYear1980)
        );
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported DataType: %s", dataType.value())
        );
    }
  }

  /**
   * Converts JPMML data into its corresponding Avro type.
   *
   * @param data to convert.
   * @param dataType of the data to convert.
   * @return a converted Avro datum.
   */
  public static Object convertFieldValueToAvro(
      final Object data,
      final DataType dataType
  ) {
    switch (dataType) {
      case STRING:
      case INTEGER:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return data;
      case DATE:
        final LocalDate date = (LocalDate) data;
        return date.toString();
      case TIME:
        final LocalTime time = (LocalTime) data;
        return time.toString();
      case DATE_TIME:
        final LocalDateTime dateTime = (LocalDateTime) data;
        return dateTime.toString();
      // Not sure if this datatype is supported by JPMML.
      case DATE_DAYS_SINCE_0:
      case DATE_DAYS_SINCE_1960:
      case DATE_DAYS_SINCE_1970:
      case DATE_DAYS_SINCE_1980:
        final DaysSinceDate daysSinceDate = (DaysSinceDate) data;
        return daysSinceDate.intValue();
      case TIME_SECONDS:
        final SecondsSinceMidnight seconds = (SecondsSinceMidnight) data;
        return seconds.intValue();
      // Not sure if this datatype is supported by JPMML.
      case DATE_TIME_SECONDS_SINCE_0:
      case DATE_TIME_SECONDS_SINCE_1960:
      case DATE_TIME_SECONDS_SINCE_1970:
      case DATE_TIME_SECONDS_SINCE_1980:
        final SecondsSinceDate secondsSinceDate = (SecondsSinceDate) data;
        return secondsSinceDate.intValue();
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported DataType: %s", dataType.value())
        );
    }
  }

  /**
   * Builds a schema for the provided PMML fields. Fields in the resulting name will match exactly
   * the provided PMML field names.
   *
   * @param recordName of the desired record schema.
   * @param fieldNames of the desired record schema.
   * @param fieldTypes of the desired record schema (in PMML types).
   * @return a record schema for the provided PMML fields.
   */
  public static Schema fieldNamesToSchema(
      final String recordName,
      final Iterable<FieldName> fieldNames,
      final Map<FieldName, DataType> fieldTypes
  ) {
    final List<Schema.Field> fields = Lists.newArrayList();
    for (FieldName fieldName : fieldNames) {
      Preconditions.checkArgument(
          fieldTypes.containsKey(fieldName),
          String.format("Missing type for field: %s", fieldName.getValue())
      );
      final Schema.Field schemaField = new Schema.Field(
          fieldName.getValue(),
          convertDataTypeToSchema(fieldTypes.get(fieldName)),
          null,
          null
      );
      fields.add(schemaField);
    }
    final Schema predictedRecord =
        Schema.createRecord(recordName, null, null, false);
    predictedRecord.setFields(fields);
    return predictedRecord;
  }

  /**
   * Builds the appropriate parameters for this score function.
   *
   * @param modelFile containing the trained PMML model.
   * @param modelName of the trained PMML model.
   * @param predictorColumn that the trained PMML model requires to generate a score.
   * @param resultRecordName of the output record to be stored from the trained PMML model.
   * @return the parameters to be used by this score function.
   */
  public static Map<String, String> parameters(
      final String modelFile,
      final String modelName,
      final KijiColumnName predictorColumn,
      final String resultRecordName
  ) {
    final Map<String, String> parameters = Maps.newHashMap();
    parameters.put(MODEL_FILE_PARAMETER, modelFile);
    parameters.put(MODEL_NAME_PARAMETER, modelName);
    parameters.put(PREDICTOR_COLUMN_PARAMETER, predictorColumn.getName());
    parameters.put(RESULT_RECORD_PARAMETER, resultRecordName);

    return parameters;
  }
}
