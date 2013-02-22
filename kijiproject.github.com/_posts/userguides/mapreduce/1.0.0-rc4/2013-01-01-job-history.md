---
layout: post
title: Job History
categories: [userguides, mapreduce, 1.0.0-rc4]
tags : [mapreduce-ug]
version: 1.0.0-rc4
order : 8
description: Job History Table.
---

### Motivation

While Hadoop’s job tracker provides detailed information about jobs that have been run in the cluster, it is not a persistent data store for such information.  After installation of the `job_history` table, Kiji automatically tracks information into the job_history table which can be accessed later.  This information includes an xml dump of the full job configuration, start times, end times, and all job counters.
/

### Setup
The job history tables can be installed into the default Kiji instance with the job-history command:

{% highlight bash %}
kiji job-history --kiji=kiji://.env/default --install
{% endhighlight %}

If you would like to install the job history table into a different instance, pass in the relevant URI into the --kiji parameter.

You can verify that the table was installed properly using the ls command:

{% highlight bash %}
kiji ls --kiji=kiji://.env/default/job_history
{% endhighlight %}

Any new MapReduce jobs that are run within Kiji will now save relevant job related metadata into this table.

### Classes Overview

The `org.kiji.mapreduce.JobHistoryKijiTable` is the main class responsible for providing access to the job_history table.  Currently it provides the ability to record and retrieve job metadata.  This is a framework-audience class and subject to change between minor versions

### Using the API

The `JobHistoryKijiTable` class surfaces the call `getJobDetails` for retrieving the recorded metadata.

Jobs that are created using `KijiMapReduceJob` will automatically record metadata to the `job_history` table if it has been installed.  Any MapReduce jobs using the job builders in Kiji for bulk importers, producers, or gatherers fall under this category and will report data.

### Example

The job_history table is a Kiji table under the hood, and can thus be inspected using the `kiji ls` tool.  The `EntityId` associated with the job_history table is the jobId.  For example, to look at all of the jobIds that have been recorded:

{% highlight bash %}
kiji ls --kiji=kiji://.env/default/job_history --columns=info:jobId
{% endhighlight %}

There is also a job_history tool, which displays the job history data in a more human readable format.  For example, to look up job data for the job ‘job_20130221123621875_0001’

{% highlight bash %}
kiji job-history --kiji=kiji://.env/default --job-id=job_20130221123621875_0001
{% endhighlight %}