---
layout: post
title: What is Scoring
categories: [userguides, scoring, 0.9.0]
tags : [scoring-ug]
order : 1
version : devel
description: Introduction to KijiScoring.
---

Scoring is the application of a trained model against data to produce an actionable result. This result could be a recommendation, classification, or other derivation.

KijiScoring is a library for scoring entity centric data with models in real time. It provides two interfaces for users to implement which describe the rules for when a model should be applied (the `KijiFreshnessPolicy`) and the model execution itself (the `ScoreFunction`) as well as one interface for requesting freshened data (`FreshKijiTableReader`).

![freshening](http://static.kiji.org/wp-content/uploads/2013/08/Untitled.png)

The conditional application of a model in KijiScoring is called 'freshening' because stale data (staleness is defined by the `KijiFreshnessPolicy` implementation, commonly data is stale if it is old enough that it does not reflect the current state of a row) from a Kiji table is updated with fresh, newly calculated data. A `KijiFreshnessPolicy`, a `ScoreFunction`, configuration parameters, and a Kiji column combine to form a 'Freshener' which is the atomic unit of freshening. A Freshener may only be attached to a fully qualified column and only one Freshener may be attached to each column at a time. Once created, Fresheners are stored in the Kiji meta table, loaded into `FreshKijiTableReader`s, and run to refresh data stored in user tables.

Freshening is a powerful tool to improve the quality and efficiency of systems which employ machine learning. Common machine learning processes involve large scale batch computation across massive data sets. These batch processes cannot be run continuously and run indiscriminately against all available data. Freshening allows for models to score up-to-the-minute data, including data that may not be available during batch computation such as the day of the week, current page views, or current weather, and avoids wasteful computation by only applying models to those entities whose data is accessed.
