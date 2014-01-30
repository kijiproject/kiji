---
layout: post
title: Next Steps
categories: [tutorials, scoring, 0.11.0]
tags : [scoring-tutorial]
version: 0.11.0
order : 7
description: Next Steps.
---
Before integrating KijiScoring into your application, you'll want to spend some time on
the system-wide design to make sure that freshening adds value and doesn't delay results
unnecessarily. Here are some considerations:

*   Performance overhead

    As with any time-sensitive operation, you'll want to design your system to do as
   little as possible between the request and getting data to the application.
   For example, consider asynchronous processes to update the data to reduce the
   likelihood of triggering freshening. KijiScoring allows you to set a timeout
   value for each freshening: how long can the freshening process take before the
   read simply takes the existing stale value. You'll want to experiment to understand typical
   response times and determine what your application can tolerate in terms of
   waiting for refreshed data.

*   Freshening multiple columns in a single read request

    KijiScoring allows you to specify if the freshened result needs to include refreshed
   values from all columns involved in the request, or if results for each column can be
   provided as they are available. Use Partial Freshening if your application uses the
   values from individual columns in the request results independently.

*   Per-request configuration information

    Apply additional information to the scoring process beyond what's stored in the
    Kiji table.

*   Cascading freshening

    Data read as input to the freshening process will not itself be freshened. This
    kind of cascading freshening would make it very difficult to guarantee desirable
    performance.