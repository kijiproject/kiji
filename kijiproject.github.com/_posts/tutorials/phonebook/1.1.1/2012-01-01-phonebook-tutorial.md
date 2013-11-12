---
layout: post
title: Overview
categories: [tutorials, phonebook-tutorial, 1.1.1]
tags: [phonebook]
order: 1
description: Quick tutorial to get up and running with Kiji Tables.
---

We assume that our incredibly popular user, **you**, needs a big-data solution
for your phone book. We will thus walk you through the process of defining a
layout for your phone book, creating a Kiji table with this layout and looking
up an entry. The current text file you have been using to store your contacts
resides at `$KIJI_HOME/examples/phonebook/input-data.txt` and we also show you how
to bulk load this into your Kiji table.

Example code for a more in depth look at these use cases exist in the directory
`$KIJI_HOME/examples/phonebook/src/main/java`.  Instructions for how to build and use this
example code can be found in the next section of this tutorial,
[Setup]({{site.tutorial_phonebook_1_1_1}}/phonebook-setup).

### How to Use this Tutorial

* **Links to Javadoc** - Class names link to the relevant Javadoc:
[`EntityId`]({{site.api_schema_1_3_3}}/EntityId.html).

* **Code Walkthrough** - Code snippets are in gray boxes with language specific syntax highlighting:

{% highlight java %}
System.out.println("Hello Kiji");
{% endhighlight %}

* **Shell Commands** - Shell commands to run the above code will be in light blue boxes, and the results in grey.

<div class="userinput">
{% highlight bash %}
echo "Hello Kiji"
{% endhighlight %}
</div>

    Hello Kiji
