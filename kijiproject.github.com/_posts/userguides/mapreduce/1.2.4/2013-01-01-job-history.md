---
layout: post
title: Job History
categories: [userguides, mapreduce, 1.2.4]
tags : [mapreduce-ug]
version: 1.2.4
order : 10
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

Jobs that extend [`KijiMapReduceJob`]({{site.api_mr_1_2_4}}/framework/KijiMapReduceJob.html) will automatically record metadata to the `job_history` table.

#### Security
For more information on Kiji security, see the KijiSchema userguide. If you have a secure Kiji
instance, KijiMR should "just work", except that users without WRITE permissions on the instance
will not have their jobs recorded in the Job History Table, and you will see a non-fatal error even
if the job ran successfully.  For example, users with only READ permissions on the instance will be
able to run Gatherers, but those jobs will not be recorded.

You can grant WRITE permissions on an instance, if you have GRANT permission, as follows:

{% highlight bash %}
kiji-schema-shell
schema > MODULE security;
schema > GRANT WRITE PRIVILEGES ON INSTANCE 'kiji://myzk:2181/myinstance' TO USER 'ada';
OK.
{% endhighlight %}

### Classes Overview

The [`JobHistoryKijiTable`]({{site.api_mr_1_2_4}}/framework/JobHistoryKijiTable.html) class is the main class responsible for providing access to
the `job_history` table.  Currently it provides the ability to record and retrieve job metadata.  This
is a framework-audience class and subject to change between minor versions.

### Using the API

The [`JobHistoryKijiTable`]({{site.api_mr_1_2_4}}/framework/JobHistoryKijiTable.html) class surfaces the calls `getJobDetails(String jobId)` and `getJobScanner()` for retrieving the recorded metadata.

### Example

The `job_history` table is a Kiji table under the hood, and can thus be inspected using the `kiji ls`, `kiji scan`, and `kiji get` tools.  The [`EntityId`]({{site.api_schema_1_3_6}}/EntityId.html) associated with the `job_history` table is the jobId.  For example, to look at all of the jobIds that have been recorded:

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
