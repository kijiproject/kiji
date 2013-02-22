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


* [KijiSchema](kiji-schema/1.0.0-rc4) (latest) 
  \[[1.0.0-rc3](kiji-schema/1.0.0-rc3)\]
  \[[1.0.0-rc2](kiji-schema/1.0.0-rc2)\]
  \[[1.0.0-rc1](kiji-schema/1.0.0-rc1)\]

* [KijiMR](kiji-mapreduce/1.0.0-rc4)
  \[[1.0.0-rc4](kiji-mapreduce/1.0.0-rc4)\]

* [KijiMR Library](kiji-mapreduce-lib/1.0.0-rc4)
  \[[1.0.0-rc4](kiji-mapreduce-lib/1.0.0-rc4)\]

## Extended Framework Tools
Additional APIs for use by clients of the Kiji framework. Modules in this section
are intended for testing, or as convenience APIs.


<ul>
  <li><a href="annotations/1.0.0-rc4">annotations</a>
      [<a href="annotations/1.0.0-rc4">1.0.0-rc2</a>]
  </li>
  <li><a href="hbase-maven-plugin/1.0.11-cdh4">hbase-maven-plugin</a>
      [<a href="hbase-maven-plugin/1.0.11-cdh4">1.0.11-cdh4</a>]
  </li>
</ul>


## Internal Framework Components
APIs used internally between framework modules. Clients should not directly
depend on any APIs in here; these are intended for the framework author audience
only.

<ul>
  <li><a href="hadoop-configurator/1.0.3">hadoop-configurator</a>
      [<a href="hadoop-configurator/1.0.3">1.0.3</a>]
  </li>
  <li><a href="kiji-delegation/1.0.0-rc4">kiji-delegation</a>
      [<a href="kiji-delegation/1.0.0-rc4">1.0.0-rc4</a>]
  </li>
</ul>
