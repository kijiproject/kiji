---
layout: post
title: Job History
categories: [userguides, mapreduce, 1.0.0-rc62]
tags : [mapreduce-ug]
version: 1.0.0-rc62
order : 9
description: Job History Table.
---

### Motivation

While Hadoop’s job tracker provides detailed information about jobs that have been run in the
cluster, it is not a persistent data store for such information. 
Kiji tracks all historical jobs in a `job_history` table within every instance. 
This information includes an xml dump of the full job configuration, start times,
end times, and all job counters.


### Setup
The `job_history` table is installed in a particular instance as soon as the first MapReduce job is run in that instance.

You can verify that the table was installed properly using the ls command:

{% highlight bash %}
kiji ls kiji://.env/default/job_history
{% endhighlight %}

Jobs that extend [`KijiMapReduceJob`]({{site.api_mr_1_0_0_rc62}}/framework/KijiMapReduceJob.html) will automatically record metadata to the `job_history` table.

### Classes Overview

The [`JobHistoryKijiTable`]({{site.api_mr_1_0_0_rc62}}/framework/JobHistoryKijiTable.html) class is the main class responsible for providing access to
the `job_history` table.  Currently it provides the ability to record and retrieve job metadata.  This
is a framework-audience class and subject to change between minor versions.

### Using the API

The [`JobHistoryKijiTable`]({{site.api_mr_1_0_0_rc62}}/framework/JobHistoryKijiTable.html) class surfaces the calls `getJobDetails(String jobId)` and `getJobScanner()` for retrieving the recorded metadata.

### Example

The `job_history` table is a Kiji table under the hood, and can thus be inspected using the `kiji ls`, `kiji scan`, and `kiji get` tools.  The [`EntityId`]({{site.api_schema_1_0_3}}/EntityId.html) associated with the `job_history` table is the jobId.  For example, to look at all of the jobIds that have been recorded:

{% highlight bash %}
kiji scan kiji://.env/default/job_history/info:jobId
{% endhighlight %}

There is also a `kiji job_history` tool, which displays the job history data in a more human readable
format.

{% highlight bash %}
kiji job-history --kiji=kiji://.env/default/
{% endhighlight %}

To look up the job data for an individual job with jobId ‘job_20130221123621875_0001’, try:

{% highlight bash %}
kiji job-history --kiji=kiji://.env/default --job-id=job_20130221123621875_0001
{% endhighlight %}
