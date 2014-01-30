---
layout: post
title: KijiFreshnessPolicy
categories: [userguides, scoring, 0.11.0]
tags : [scoring-ug]
order : 2
version : devel
description: Description of KijiFreshnessPolicy SPI.
---

<div id="accordion-container">
  <h2 class="accordion-header"> KijiFreshnessPolicy.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring/raw/kiji-scoring-root-0.11.0/src/main/java/org/kiji/scoring/KijiFreshnessPolicy.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> KijiFreshnessPolicy </h3>
A `KijiFreshnessPolicy` governs the execution of a model by answering the question: “Is requested data fresh or stale?” If data is fresh, a new score is not needed and the current fresh data will be returned. If data is stale, a new score is needed and a `ScoreFunction` will run to generate that score. Most methods of a `KijiFreshnessPolicy` are helpers to provide input to the main method, `isFresh`, which answers this question.

Methods of a `KijiFreshnessPolicy` are divided into three categories based on when and where they are called.

Attachment time methods are called during Freshener registration in a `KijiFreshnessManager`.

1. `serializeToParameters` allows a policy to save its state into into the Freshener record so that it can be rebuilt in a fresh reader. An example of how this is used can be found in the stock `KijiFreshnessPolicy` provided in org.kiji.scoring.lib, ShelfLife which provides a convenience constructor and uses `serializeToParameters` to store the input values from that constructor into the Freshener record so that it is preserved while the Freshener is serialized.

Setup and cleanup methods are called when a Freshener is loaded or unloaded from a `FreshKijiTableReader`.

1. `getRequiredStores(FreshenerGetStoresContext)` allows a policy to describe the `KeyValueStore`s it requires. All methods called after this, including `setup`, `cleanup`, and all request time methods can access these `KeyValueStore`s. `KeyValueStore`s can be used to access side data, for instance from a foreign Kiji table.
2. `setup(FreshenerSetupContext)` configures the internal state of the policy.
3. `cleanup(FreshenerSetupContext)` cleans up resources used by the policy.

Request time methods are called while a Freshener is live in a `FreshKijiTableReader` in response to every read request which includes the column to which the Freshener is attached.

1. `shouldUseClientDataRequest(FreshenerContext)` allows the policy to choose what entity data it sees in `isFresh`. If this returns true, the row data passed to `isFresh` will be defined by the client’s data request which triggered this freshening.
2. `getDataRequest(FreshenerContext)` specifies data to be passed to `isFresh` if `shouldUseClientDataRequest` returns false.
3. `isFresh(KijiRowData, FreshenerContext)` determines if data is fresh or stale.

KijiScoring provides several stock implementations of `KijiFreshnessPolicy` in the org.kiji.scoring.lib package. Included are: AlwaysFreshen, which returns false to `isFresh` in all cases; NeverFreshen, which returns true to `isFresh` in all cases; NewerThan, which returns true if requested data was written more recently than a configurable time; ShelfLife, which returns true if requested data was written within a configurable amount of time from the current time.
