---
layout: post
title: Overview
categories: [tutorials, music-recommendation, 1.0.0-rc61]
tags: [music]
order: 1
description: A tutorial to get you using MapReduce with Kiji Tables.
---

It’s the year 3000, and new music is being created at such a fast pace that your company Pandorify
can no longer analyze and categorize music fast enough to recommend it to your users.  Fortunately,
technology has kept up! With KijiSchema and KijiMR, you can leverage all the song play data you have
collected over the last thousand years to make recommendations for your users.

We believe that past patterns are a good source for future recommendations. To impose structure on
the data, we will use a layout in KijiSchema. To effectively use this gigantic flood of data, we
can use MapReduce paradigms provided by KijiMR to analyze and provide recommendations.

In this tutorial, we demonstrate how to use KijiMR to leverage your data effectively. You will:

* Efficiently import data into a KijiTable.
* Manipulate data in and between Kiji and HDFS.
* Use a [gatherer]({{ site.userguide_mapreduce_1_0_0_rc62 }}/gatherers) to generate the next-song pairs,
  and use a generic MapReduce job to aggregate them into counts.
* Use a [producer]({{ site.userguide_mapreduce_1_0_0_rc62 }}/producers) and join together data sources, to
  generate the recommendations for our users.


### How to Use this Tutorial

* **Links to Javadoc** - Class names link to the relevant Javadoc:
[`EntityId`]({{site.api_schema_1_0_3}}/EntityId.html).

* **Code Walkthrough** - Code snippets are in gray boxes with language specific syntax highlighting.

{% highlight java %}
System.out.println("Hello Kiji");
{% endhighlight %}

* **Shell Commands** - Shell commands to run the above code will be in light blue boxes, and the
+results in grey.

<div class="userinput">
{% highlight bash %}
echo "Hello Kiji"
{% endhighlight %}
</div>

    Hello Kiji

* **Expandable Code** - Larger sections of code are marked with headers in gray boxes that contain a
+. These can be expanded by clicking the header.

<div id="accordion-container">
  <h2 class="accordion-header"> HelloWorld.java </h2>
     <div class="accordion-content">
{% highlight java %}
public class HelloWorld {
  public static void main(String[] args) {
    System.out.println("Hello world!");
  }
}
{% endhighlight %}
  </div>
</div>
