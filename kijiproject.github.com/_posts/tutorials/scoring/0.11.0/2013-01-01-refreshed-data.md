---
layout: post
title: Accessing the Refreshed Data
categories: [tutorials, scoring, 0.11.0]
tags : [scoring-tutorial]
version: 0.11.0
order : 6
description: Accessing the Refreshed Data.
---
Now that the you have registered a freshener for your column, you can construct a
`FreshKijiTableReader` to use it. We've provided a sample tool that simulates tracking
a bunch of people listening to song after song. It reads the recommendations column that
triggers freshening.

The following reader is provided as part of the
scoring-music CLI tool:

    // Open a FreshKijiTableReader for the table with a timeout of 1 second.
    final FreshKijiTableReader freshReader = FreshKijiTableReader.Builder.create()
        .withTable(userTable)
        .withTimeout(1000)
        .build();

To use this tool to see freshening in action, run the following command:

<div class="userinput">
{% highlight bash %}
kiji scoring-music --kiji=${KIJI}/users \
--write-user=user-35 \
--freshen-user=user-35
{% endhighlight %}
</div>

This command writes a new random track play for `user-35` and then freshens the
recommendation for that user. If you run this command multiple times, or for different users,
you should see new recommendations generated continuously.

To add freshening to any Kiji application, simply create and attach your
`KijiFreshnessPolicy` and `ScoreFunction` implementations to the columns you want
freshened and access data in those columns through a `FreshKijiTableReader` as shown above.

