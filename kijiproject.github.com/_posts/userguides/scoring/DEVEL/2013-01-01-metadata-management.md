---
layout: post
title: Metadata Management
categories: [userguides, scoring, devel]
tags : [scoring-ug]
order : 7
version : devel
description: Description of managing metadata in KijiScoring.
---

<div id="accordion-container">
  <h2 class="accordion-header"> KijiFreshnessManager.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring/raw/{{site.scoring_devel_branch}}/src/main/java/org/kiji/scoring/KijiFreshnessManager.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> KijiFreshnessManager </h3>
The KijiFreshnessManager manages storage, and validation of Freshener configurations. All reads and writes of Freshener configuration to the Kiji meta table go through a KijiFreshnessManager. KijiFreshenerManager methods for registering Fresheners all provide a set of switches which control how the Freshener will be validated and what methods will be called during attachment to generate the final configuration which is stored to the Kiji meta table. A value is required for each switch for every registration.

1. Overwrite existing. If true, this option instructs the manager to write the new Freshener even if there is already a Freshener attached to the selected column. If this option is set to false, the manager will return a validation failure exception if there is a Freshener already attached to the column.
2. Instantiate classes. Setting this option to true instructs the manager to instantiate the KijiFreshnessPolicy and ScoreFunction classes specified in a Freshener and call their serializeToParameters() methods before saving the record configuration. The output of serialization is included in the Freshener configuration. If serialize methods or the parameters specified during registration have key collisions the following priority is used to resolve them: ScoreFunction.serializeToParameters(), KijiFreshnessPolicy.serializeToParameters(), manually specified parameters from lowest to highest priority. Use of this option requires that classes are available on the classpath.
3. Setup classes. This option has no effect if instantiate classes is false. If true, this option causes the manager to build a temporary FreshenerSetupContext object with which to call the setup methods of both policy and score function. If used, the classes will be setup before their serializeToParameters methods are called. This option should be used only if serializeToParameters requires classes to be setup before they can be serialized.

When registering a Freshener and on calls to explicit validation methods a KijiFreshnessManager can ensure that a Freshener record conforms to requirements for normal operation. A manager can validate that the attached column is fully qualified, that the policy and score function class names are legal Java class identifiers, that there is not a Freshener already attached to the column (this check is disabled if overwrite existing is set when registering and is never run when validating an existing attached Freshener), and that the attached column exists in the Kiji table to which the Freshener is being attached.

<h3 style="margin-top:0px;padding-top:10px;"> FreshTool </h3>
The Fresh tool replicates the functionality of the KijiFreshnessManager on the command line.

<table  border="1">
  <tr><td>--target</td><td>The KijiURI of the target of the operation. Must include at least an instance and table, and may include a column.</td></tr>
  <tr><td>--do</td><td>pick exactly one of:<br>
* ‘register’ to register a Freshener for the column specified in --target. Requires --policy-class, --score-function-class, exactly one column in --target. Optionally accepts --parameters, --instantiate-classes, --overwrite-existing.<br>
* ‘retrieve’ to retrieve the Freshener record(s) for the specified table or column.<br>
* ‘remove’ to remove the Freshener record(s) from the specified table or column.<br>
* ‘validate’ to run validation on the Freshener record(s) attached to the specified table or column.</td></tr>
  <tr><td>--policy-class</td><td>Fully qualified class name of the KijiFreshnessPolicy implementation to include in the Freshener. Required for registration, may not be specified for other --do options.</td></tr>
  <tr><td>--score-function-class</td><td>Fully qualified class name of the ScoreFunction implementation to include in the Freshener. Required for registration, may not be specified for other --do options.</td></tr>
  <tr><td>--parameters</td><td>JSON encoded mapping of configuration parameters to include in the Freshener. Optional for registration, may not be specified for other --do options. Defaults to empty if unspecified.</td></tr>
  <tr><td>--instantiate-classes</td><td>Instructs the tool to instantiate classes to call their serializeToParameters methods to include those parameters in the registered Freshener. May only be specified with --do=register. Defaults to false.</td></tr>
  <tr><td>--overwrite-existing</td><td>Instructs the tool to overwrite an existing Freshener when registering. May only be specified with --do=register. Defaults to false.</td></tr>
  <tr><td>--setup-classes</td><td>Instructs the tool to call setup on the KijiFreshnessPolicy and ScoreFunction objects created if --instantiate-classes=true. The tool will construct a FreshenerSetupContext using other parameteres specified here. Defaults to false.</td></tr>
</table>

Example usage:

Register a new Freshener to table/family:qualifier

    kiji fresh --target=kiji://.env/default/table/family:qualifier \
        --do=register \
        --policy-class=com.mycompany.Policy \
        --score-function-class=com.mycompany.ScoreFunction \
        --parameters=’{“key1”:”value1”,”key2”:”value2”}’

Retrieve and print the Freshener attached to table/family:qualifier

    kiji fresh --target=kiji://.env/default/table/family:qualifier \
        --do=retrieve

Retrieve and print the Fresheners attached to columns in table

    kiji fresh --target=kiji://.env/default/table \
        --do=retrieve

Remove the Freshener attached to table/family:qualifier

    kiji fresh --target=kiji://.env/default/table/family:qualifier \
        --do=remove

Validate Fresheners attached to columns in table and print the results.

    kiji fresh --target=kiji://.env/default/table \
        --do=validate
