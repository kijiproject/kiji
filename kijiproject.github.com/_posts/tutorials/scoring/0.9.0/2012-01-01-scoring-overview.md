---
layout: post
title: Scoring Tutorial Overview
categories: [tutorials, scoring, devel]
tags : [scoring-tutorial]
version: devel
order : 1
description: KijiScoring Tutorial Overview.
---
Scoring is the act of applying a trained model to input data to produce an actionable
result such as a categorization (fraud detection) or the next step in a pattern
(retail product recommendation). Broadly interpreted, scoring could be something
as sophisticated as applying a trained linear regression model or as straightforward
as extracting the domain from an email address.

While it is possible to apply models as a batch process, there are two main reasons why
you might want to do this in real time:

1. Lazy evaluation saves time and processing. You only score those entries you read.
2. Some information is available only in real time. For example, you may want to apply a
   different model based on the day of the week or the weather.

Scoring in a Kiji system is the real-time or
on-demand application of a trained model to input data to produce an actionable result.
KijiScoring makes these real-time, entity-centric calculations possible
using a technique called freshening, which allows you to lazily apply a calculation to
entity-centric data to produce scored results on read without launching an entire
MapReduce job. You would identify a "freshness policy" that specified how you want the
data to be freshened, such as always, never, or after a set interval. When your
application attempts to read from a column that has a
freshness policy and model associated with it, KijiScoring triggers the freshening
process to run before the read is complete.

This tutorial shows a simple example of when real-time scoring excels over batch
processing. It walks you through the steps to set up KijiScoring, building on the
simple music recommendation scenario that the KijiMR Music Recommendation Tutorial
implements. The Music Recommendation tutorial runs a batch process that recommends
the best song based on what a user just listened to. Because it's a batch process,
you're stuck with that recommendation until you run the batch process over again.
In the time between when one song ends and the next song starts you may not have a
valuable recommendation.

Modifying the scenario to use KijiScoring, you can defer the calculation to the
last second. You can make a recommendation based on what the user just listened to,
meaning they get the best recommendation every time.

The KijiScoring tutorial includes:

*  [Setting up the KijiScoring components](../scoring-setup) on a machine that has access
   to the standalone Kiji environment where the data is stored in Kiji tables.
*  Choosing a [KijiFreshnessPolicy](../freshness-policy) and [ScoreFunction](../score-function).
*  [Registering the KijiFreshnessPolicy and ScoreFunction](../registering) to a Kiji table column.
*  [Accessing the refreshed column data](../refreshed-data) via a `FreshKijiTableReader`.

If you haven't already worked through the
[Music Recommendation Tutorial]({{site.tutorial_music_devel}}/music-overview/), you'll
want to do that first.

