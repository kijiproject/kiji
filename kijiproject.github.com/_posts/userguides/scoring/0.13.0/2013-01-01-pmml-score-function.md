---
layout: post
title: PMML ScoreFunction
categories: [userguides, scoring, 0.13.0]
tags : [scoring-ug]
order : 8
version : devel
description: Description of PMML compliant ScoreFunction.
---

<div id="accordion-container">
  <h2 class="accordion-header"> JpmmlScoreFunction.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring/raw/kiji-scoring-root-0.13.0/src/main/java/org/kiji/scoring/lib/JpmmlScoreFunction.java"> </script>
  </div>
</div>


<h3 style="margin-top:0px;padding-top:10px;"> JpmmlScoreFunction </h3>
The JpmmlScoreFunction provides a way to deploy a [PMML](http://www.dmg.org/pmml-v4-2.html) compliant model that has been trained externally to the Kiji ecosystem to the Kiji model repository. The Kiji scoring server will calculate scores. This score function internally uses the [Jpmml](https://github.com/jpmml) library to parse and evaluate PMML models.

A deployed JpmmlScoreFunction expects that the table it is attached to has:

* One column containing a record with one field per predictor [MiningField](http://www.dmg.org/v4-2/MiningSchema.html).
* One column containing a record that will be the output from the model (one record field per predicted/output field).
* The type of the fields in both the predictor record and result record must match the provided type in the pmml [DataDictionary](http://www.dmg.org/v4-2/DataDictionary.html).
* If the output column is setup with `STRICT` schema validation the schema that will be produced by the pmml model must be registered as a writer (you may also want to register the predictor column's expected reader schema).

The following additional constraints must be met to use the JpmmlScoreFunction:

<!--- TODO(DOCS-161): Add list of actually supported models. -->

* Must use a model that is supported: [https://github.com/jpmml/jpmml-evaluator](https://github.com/jpmml/jpmml-evaluator) (see [README](https://github.com/jpmml/jpmml-evaluator/blob/master/README.md)).
* Must already have a trained model xml file ([example](https://github.com/kijiproject/kiji-scoring/blob/master/src/test/resources/simple-linear-regression.xml)).
* Model must not use [PMML extensions](http://www.dmg.org/v4-2/GeneralStructure.html#xsdElement_Extension).
* Model must not operate on sets (association rules models).


<h3 style="margin-top:0px;padding-top:10px;"> Pmml-Avro Data Type Mapping </h3>
The JpmmlScoreFunction attempts to convert pmml data types into corresponding avro data types (if specified). Collections are currently not supported. If the type of a field is not specified, a best effort conversion will take place.

<table border="1">
  <tr>
    <td>PMML Data Type</td>
    <td>Avro Data Type</td>
    <td>Notes</td>
  </tr>
  <tr>
    <td>STRING</td>
    <td>string</td>
    <td></td>
  </tr>
  <tr>
    <td>INTEGER</td>
    <td>long</td>
    <td>Pmml has no "long" type</td>
  </tr>
  <tr>
    <td>FLOAT</td>
    <td>float</td>
    <td></td>
  </tr>
  <tr>
    <td>DOUBLE</td>
    <td>double</td>
    <td></td>
  </tr>
  <tr>
    <td>BOOLEAN</td>
    <td>boolean</td>
    <td></td>
  </tr>
  <tr>
    <td>DATE/TIME/DATE_TIME</td>
    <td>string</td>
    <td>Formatted using ISO8601</td>
  </tr>
  <tr>
    <td>
      DATE_DAYS_SINCE_0<br />
      DATE_DAYS_SINCE_1960<br />
      DATE_DAYS_SINCE_1970<br />
      DATE_DAYS_SINCE_1980<br />
      TIME_SECONDS<br />
      DATE_TIME_SECONDS_SINCE_0<br />
      DATE_TIME_SECONDS_SINCE_1960<br />
      DATE_TIME_SECONDS_SINCE_1970<br />
      DATE_TIME_SECONDS_SINCE_1980<br />
    </td>
    <td>int</td>
    <td>JPMML only provides these as integers.</td>
  </tr>
</table>


<h3 style="margin-top:0px;padding-top:10px;"> Deploying a JpmmlScoreFunction with the 'model-repo pmml' tool </h3>

1. Place the pmml xml file in a location accessible to your account on hdfs or your local filesystem.

2. Ensure that the field names in the pmml xml file are valid Avro field names and that your model's name is of the form: artifact.model-version.

3. Generate a model container descriptor by running the model-repo pmml tool against the generated pmml xml file with syntax:

        kiji model-repo pmml \
            --table=kiji://my/kiji/table \
            --model-file=file:///path/to/pmml/xml/file.xml \
            --model-name=nameOfModelInPmmlFile \
            --model-version=0.0.1 \
            --predictor-column=model:modelpredictor \
            --result-column=model:modelresult \
            --result-record-name=MyModelResult \
            --model-container=/path/to/write/model-container.json

4. Create an empty jar (can't deploy right now without a jar file and JpmmlScoreFunction lives in the kiji-scoring jar):

        touch empty-file
        jar cf /path/to/empty-jar.jar empty-file
        rm empty-file

5. Deploy the generated model container. The deps-resolver flag must be specified here even though it won't get used ([KIJIREPO-47](https://jira.kiji.org/browse/KIJIREPO-47)):

        kiji model-repo deploy nameOfModelInPmmlFile /path/to/empty-jar.jar \
            --kiji=kiji://my-model-repo/instance \
            --deps-resolver=maven \
            --production-ready=true \
            --model-container=/path/to/written/model-container.json \
            --message="Initial deployment of JPMML based model."

6. Attach the JpmmlScoreFunction to the result column for the model (requires an active [scoring server](https://github.com/kijiproject/kiji-scoring-server)):

        # Freshness policy may need to be different depending on model.
        # This command will fail if the model's name is not of the form: artifact.model-version.
        kiji model-repo fresh-model \
            kiji://my-model-repo/instance \
            nameOfModelInPmmlFile \
            org.kiji.scoring.lib.AlwaysFreshen

After running through these steps, your model should be running/active. To validate that your newly deployed model is functioning:

    # List active ScoreFunctions in the scoring server by model repo name and version.
    curl <scoring.server.hostname>:<scoring-server-port>/admin/list
