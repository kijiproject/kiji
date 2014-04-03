---
layout: page
title: API Reference
---

The Kiji framework contains several components; documentation for their
APIs is provided through the links below.

## Core Kiji Framework Modules

Primary APIs for use by clients of the Kiji framework. Clients should
only depend on APIs listed as <tt>@ApiAudience.Public</tt>. Contributors
to other Kiji framework components may use <tt>@ApiAudience.Framework</tt>
APIs, but beware that they may change between minor versions.

* [KijiSchema](http://api-docs.kiji.org/kiji-schema/1.4.0/index.html) (latest)
  \[[1.3.7](http://api-docs.kiji.org/kiji-schema/1.3.7/index.html)\]
  \[[1.3.6](http://api-docs.kiji.org/kiji-schema/1.3.6/index.html)\]
  \[[1.3.4](http://api-docs.kiji.org/kiji-schema/1.3.4/index.html)\]
  \[[1.3.3](http://api-docs.kiji.org/kiji-schema/1.3.3/index.html)\]
  \[[1.3.2](http://api-docs.kiji.org/kiji-schema/1.3.2/index.html)\]
  \[[1.3.1](http://api-docs.kiji.org/kiji-schema/1.3.1/index.html)\]
  \[[1.3.0](http://api-docs.kiji.org/kiji-schema/1.3.0/index.html)\]
  \[[1.2.1](http://api-docs.kiji.org/kiji-schema/1.2.1/index.html)\]
  \[[1.2.0](http://api-docs.kiji.org/kiji-schema/1.2.0/index.html)\]
  \[[1.1.2](http://api-docs.kiji.org/kiji-schema/1.1.2/index.html)\]
  \[[1.1.1](http://api-docs.kiji.org/kiji-schema/1.1.1/index.html)\]
  \[[1.1.0](http://api-docs.kiji.org/kiji-schema/1.1.0/index.html)\]
  \[[1.0.4](http://api-docs.kiji.org/kiji-schema/1.0.4/index.html)\]
  \[[1.0.3](http://api-docs.kiji.org/kiji-schema/1.0.3/index.html)\]
  \[[1.0.2](http://api-docs.kiji.org/kiji-schema/1.0.2/index.html)\]
  \[[1.0.1](http://api-docs.kiji.org/kiji-schema/1.0.1/index.html)\]
  \[[1.0.0](http://api-docs.kiji.org/kiji-schema/1.0.0/index.html)\]
  \[[1.0.0-rc5](http://api-docs.kiji.org/kiji-schema/1.0.0-rc5/index.html)\]
  \[[1.0.0-rc4](http://api-docs.kiji.org/kiji-schema/1.0.0-rc4/index.html)\]
  \[[1.0.0-rc3](http://api-docs.kiji.org/kiji-schema/1.0.0-rc3/index.html)\]
  \[[1.0.0-rc2](http://api-docs.kiji.org/kiji-schema/1.0.0-rc2/index.html)\]
  \[[1.0.0-rc1](http://api-docs.kiji.org/kiji-schema/1.0.0-rc1/index.html)\]

* [KijiMR](http://api-docs.kiji.org/kiji-mapreduce/1.3.0/index.html) (latest)
  \[[1.2.5](http://api-docs.kiji.org/kiji-mapreduce/1.2.5/index.html)\]
  \[[1.2.4](http://api-docs.kiji.org/kiji-mapreduce/1.2.4/index.html)\]
  \[[1.2.3](http://api-docs.kiji.org/kiji-mapreduce/1.2.3/index.html)\]
  \[[1.2.2](http://api-docs.kiji.org/kiji-mapreduce/1.2.2/index.html)\]
  \[[1.2.1](http://api-docs.kiji.org/kiji-mapreduce/1.2.1/index.html)\]
  \[[1.2.0](http://api-docs.kiji.org/kiji-mapreduce/1.2.0/index.html)\]
  \[[1.1.0](http://api-docs.kiji.org/kiji-mapreduce/1.1.0/index.html)\]
  \[[1.0.2](http://api-docs.kiji.org/kiji-mapreduce/1.0.2/index.html)\]
  \[[1.0.1](http://api-docs.kiji.org/kiji-mapreduce/1.0.1/index.html)\]
  \[[1.0.0](http://api-docs.kiji.org/kiji-mapreduce/1.0.0/index.html)\]
  \[[1.0.0-rc62](http://api-docs.kiji.org/kiji-mapreduce/1.0.0-rc62/index.html)\]
  \[[1.0.0-rc61](http://api-docs.kiji.org/kiji-mapreduce/1.0.0-rc61/index.html)\]
  \[[1.0.0-rc6](http://api-docs.kiji.org/kiji-mapreduce/1.0.0-rc6/index.html)\]
  \[[1.0.0-rc5](http://api-docs.kiji.org/kiji-mapreduce/1.0.0-rc5/index.html)\]
  \[[1.0.0-rc4](http://api-docs.kiji.org/kiji-mapreduce/1.0.0-rc4/index.html)\]

* [KijiMR Library](http://api-docs.kiji.org/kiji-mapreduce-lib/1.1.5/index.html) (latest)
  \[[1.1.4](http://api-docs.kiji.org/kiji-mapreduce-lib/1.1.4/index.html)\]
  \[[1.1.3](http://api-docs.kiji.org/kiji-mapreduce-lib/1.1.3/index.html)\]
  \[[1.1.2](http://api-docs.kiji.org/kiji-mapreduce-lib/1.1.2/index.html)\]
  \[[1.1.1](http://api-docs.kiji.org/kiji-mapreduce-lib/1.1.1/index.html)\]
  \[[1.1.0](http://api-docs.kiji.org/kiji-mapreduce-lib/1.1.0/index.html)\]
  \[[1.0.0](http://api-docs.kiji.org/kiji-mapreduce-lib/1.0.0/index.html)\]
  \[[1.0.0-rc61](http://api-docs.kiji.org/kiji-mapreduce-lib/1.0.0-rc61/index.html)\]
  \[[1.0.0-rc6](http://api-docs.kiji.org/kiji-mapreduce-lib/1.0.0-rc6/index.html)\]
  \[[1.0.0-rc5](http://api-docs.kiji.org/kiji-mapreduce-lib/1.0.0-rc5/index.html)\]
  \[[1.0.0-rc4](http://api-docs.kiji.org/kiji-mapreduce-lib/1.0.0-rc4/index.html)\]

* [KijiExpress](http://api-docs.kiji.org/kiji-express/2.0.0-rc1/index.html) (latest)
  \[[1.0.2](http://api-docs.kiji.org/kiji-express/1.0.2/index.html)\]
  \[[1.0.1](http://api-docs.kiji.org/kiji-express/1.0.1/index.html)\]
  \[[1.0.0](http://api-docs.kiji.org/kiji-express/1.0.0/index.html)\]
  \[[0.14.0](http://api-docs.kiji.org/kiji-express/0.14.0/index.html)\]
  \[[0.13.0](http://api-docs.kiji.org/kiji-express/0.13.0/index.html)\]
  \[[0.12.0](http://api-docs.kiji.org/kiji-express/0.12.0/index.html)\]
  \[[0.10.0](http://api-docs.kiji.org/kiji-express/0.10.0/index.html)\]
  \[[0.9.0](http://api-docs.kiji.org/kiji-express/0.9.0/index.html)\]
  \[[0.8.1](http://api-docs.kiji.org/kiji-express/0.8.1/index.html)\]
  \[[0.8.0](http://api-docs.kiji.org/kiji-express/0.8.0/index.html)\]
  \[[0.7.0](http://api-docs.kiji.org/kiji-express/0.7.0/index.html)\]
  \[[0.6.0](http://api-docs.kiji.org/kiji-express/0.6.0/index.html)\]
  \[[0.5.0](http://api-docs.kiji.org/kiji-express/0.5.0/index.html)\]
  \[[0.4.0](http://api-docs.kiji.org/kiji-express/0.4.0/index.html)\]
  \[[0.3.0](http://api-docs.kiji.org/kiji-express/0.3.0/index.html)\]
  \[[0.2.0](http://api-docs.kiji.org/kiji-express/0.2.0/index.html)\]
  \[[0.1.0](http://api-docs.kiji.org/kiji-express/0.1.0/index.html)\]

* [KijiScoring](http://api-docs.kiji.org/kiji-scoring/0.13.0/index.html) (latest)
  \[[0.12.0](http://api-docs.kiji.org/kiji-scoring/0.12.0/index.html)\]
  \[[0.11.0](http://api-docs.kiji.org/kiji-scoring/0.11.0/index.html)\]
  \[[0.10.0](http://api-docs.kiji.org/kiji-scoring/0.10.0/index.html)\]
  \[[0.9.0](http://api-docs.kiji.org/kiji-scoring/0.9.0/index.html)\]
  \[[0.7.0](http://api-docs.kiji.org/kiji-scoring/0.7.0/index.html)\]
  \[[0.6.0](http://api-docs.kiji.org/kiji-scoring/0.6.0/index.html)\]
  \[[0.5.0](http://api-docs.kiji.org/kiji-scoring/0.5.0/index.html)\]
  \[[0.4.0](http://api-docs.kiji.org/kiji-scoring/0.4.0/index.html)\]
  \[[0.3.0](http://api-docs.kiji.org/kiji-scoring/0.3.0/index.html)\]

## Extended Framework Tools
Additional APIs for use by clients of the Kiji framework. Modules in this section
are intended for testing, or as convenience APIs.

<ul>
  <li><a href="http://api-docs.kiji.org/annotations/1.0.2/index.html">annotations</a>
      [<a href="http://api-docs.kiji.org/annotations/1.0.1/index.html">1.0.1</a>]
      [<a href="http://api-docs.kiji.org/annotations/1.0.0/index.html">1.0.0</a>]
      [<a href="http://api-docs.kiji.org/annotations/1.0.0-rc5/index.html">1.0.0-rc5</a>]
      [<a href="http://api-docs.kiji.org/annotations/1.0.0-rc4/index.html">1.0.0-rc4</a>]
      [<a href="http://api-docs.kiji.org/annotations/1.0.0-rc3/index.html">1.0.0-rc3</a>]
      [<a href="http://api-docs.kiji.org/annotations/1.0.0-rc2/index.html">1.0.0-rc2</a>]
  </li>
  <li><a href="http://api-docs.kiji.org/hbase-maven-plugin/1.0.11-cdh4/index.html">hbase-maven-plugin</a>
      [<a href="http://api-docs.kiji.org/hbase-maven-plugin/1.0.11-cdh4/index.html">1.0.11-cdh4</a>]
  </li>
</ul>


## Internal Framework Components
APIs used internally between framework modules. Clients should not directly
depend on any APIs in here; these are intended for the framework author audience
only.

<ul>
  <li><a href="http://api-docs.kiji.org/hadoop-configurator/1.0.3/index.html">hadoop-configurator</a>
      [<a href="http://api-docs.kiji.org/hadoop-configurator/1.0.3/index.html">1.0.3</a>]
      [<a href="http://api-docs.kiji.org/hadoop-configurator/1.0.2/index.html">1.0.2</a>]
  </li>
  <li><a href="http://api-docs.kiji.org/kiji-delegation/1.0.2/index.html">kiji-delegation</a>
      [<a href="http://api-docs.kiji.org/kiji-delegation/1.0.1/index.html">1.0.1</a>]
      [<a href="http://api-docs.kiji.org/kiji-delegation/1.0.0/index.html">1.0.0</a>]
      [<a href="http://api-docs.kiji.org/kiji-delegation/1.0.0-rc5/index.html">1.0.0-rc5</a>]
      [<a href="http://api-docs.kiji.org/kiji-delegation/1.0.0-rc4/index.html">1.0.0-rc4</a>]
      [<a href="http://api-docs.kiji.org/kiji-delegation/1.0.0-rc3/index.html">1.0.0-rc3</a>]
      [<a href="http://api-docs.kiji.org/kiji-delegation/1.0.0-rc2/index.html">1.0.0-rc2</a>]
  </li>
</ul>


