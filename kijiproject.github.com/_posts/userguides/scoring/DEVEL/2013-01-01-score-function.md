---
layout: post
title: ScoreFunction
categories: [userguides, scoring, devel]
tags : [scoring-ug]
order : 3
version : devel
description: Description of ScoreFunction SPI.
---

<div id="accordion-container">
  <h2 class="accordion-header"> ScoreFunction.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring/raw/{{site.scoring_devel_branch}}/src/main/java/org/kiji/scoring/ScoreFunction.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> ScoreFunction </h3>
A `ScoreFunction` operates on data from a Kiji row and optional side data to produce a new value for that row. This operation will run according to a `KijiFreshnessPolicy` and its outputs will be committed to the Kiji table in which the row resides and returned to the requesting client by the `FreshKijiTableReader` implementation.

Methods of a `ScoreFunction` are divided into three categories based on when and where they are called.

Attachment time methods are called during Freshener registration in a `KijiFreshnessManager`.

1. `serializeToParameters` allows a `ScoreFunction` to save its state into into the Freshener record so that it can be rebuilt in a fresh reader.

Setup and cleanup methods are called when a Freshener is loaded or unloaded from a `FreshKijiTableReader`.

1. `getRequiredStores(FreshenerGetStoresContext)` allows a `ScoreFunction` to describe the `KeyValueStores` it requires.
2. `setup(FreshenerSetupContext)` configures the internal state of the `ScoreFunction`.
3. `cleanup(FreshenerSetupContext)` cleans up resources used by the `ScoreFunction`.

Request time methods are called while a Freshener is live in a `FreshKijiTableReader` in response to every read request which includes the column to which the Freshener is attached. These methods are only called if the associated `KijiFreshnessPolicy` indicated that data was stale.

1. `getDataRequest(FreshenerContext)` specifies data to be passed to `score`.
2. `score(KijiRowData, FreshenerContext)` produces a fresh score from the row’s data. The `score`method returns a special object called a `TimestampedValue` which allows the user to specify what timestamp the value should be written to. `TimestampedValue` is a generic parameterized type which allows the user to safely return any type of value and the Kiji framework will handle it correctly.

