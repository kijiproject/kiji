---
layout: post
title: FreshenerContext
categories: [userguides, scoring, devel]
tags : [scoring-ug]
order : 4
version : devel
description: Description of FreshenerContext API.
---

<div id="accordion-container">
  <h2 class="accordion-header"> FreshenerContext.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/kijiproject/kiji-scoring/raw/{{site.scoring_devel_branch}}/src/main/java/org/kiji/scoring/FreshenerContext.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> FreshenerContext </h3>
The `KijiFreshnessPolicy` and `ScoreFunction` interfaces are designed to enable easy development of robust, thread-safe code. One of the main ways in which the interfaces accomplish this is through the use of context objects to expose environmental state to computation. By bundling state into context objects, a Freshener may be written which requires no internal fields, which are common causes of thread unsafety. Data from each invocation of a Freshener is isolated from other invocations through this abstraction. Within one request, all request time methods of both the policy and score function will have access to the same `FreshenerContext` instance.

An example of using context objects to protect states can be found in the stock `KijiFreshnessPolicy` implementations ShelfLife and NewerThan. Both of these objects require a single `long` value to define their behavior. A simple implementation of these classes might use a member field to hold the `long` which would be referenced concurrently by all threads using that policy. If the value cannot be modified, this is thread-safe and works fine, but if we wish to modify the value for just one request we run the risk of changing the behavior of other threads without knowing it. By including the value as one of our context parameters instead of a member variable we isolate each thread from others and can override the value of the `long` safely, without endangering other threads.

There are two narrower versions of `FreshenerContext` called `FreshenerGetStoresContext` and `FreshenerSetupContext` for use when the full set of state of a `FreshenerContext` is unavailable. These interfaces provide a strict subset of `FreshenerContext`’s methods and the behavior of the methods they do expose is identical to `FreshenerContext`.
