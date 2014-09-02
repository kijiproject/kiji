---
layout: post
title: Delete Contacts
categories: [tutorials, phonebook-tutorial, devel]
tags: [phonebook]
order: 8
description: Examples of Point deletions.
---

Deletions of Kiji table cells can be performed both within a MapReduce job and from
non-distributed java programs. Both types of programs use [`KijiTableWriter`]({{site.api_schema_devel}}/KijiTableWriter.html)s to
delete data.

## Point Deletions

You realize one of your frenemies, Renuka Apte (any resemblance to real persons, living or dead,
is purely coincidental), has somehow made it to your contact list. To remedy this we will
perform a point deletion on the row with Renuka's contact information. To permit deletions
from the phonebook, we will create a tool that will let us specify contacts that we want
to delete.

### DeleteEntry.java

DeleteEntry uses a [`KijiTableWriter`]({{site.api_schema_devel}}/KijiTableWriter.html) to perform point deletions on a kiji table:

{% highlight java %}
// Connect to the Kiji table and open a writer.
kiji = Kiji.Factory.open(KijiURI.newBuilder(mKijiUri).build(), getConf());
table = kiji.openTable(TABLE_NAME);
writer = table.openTableWriter();
{% endhighlight %}

The deletion is then performed by specifying the row ID for the entry, in this case
a string of the format `firstname,lastname`:

{% highlight java %}
// Create a row ID with the first and last name.
EntityId user = table.getEntityId(first + "," + last);

// Delete the row for the specified user.
writer.deleteRow(user);
{% endhighlight %}

### Running the Example

This example interactively queries the user for the first and last names of the contact
to delete. First, verify that the contact entry for Renuka Apte exists in your phonebook
table:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji get ${KIJI}/phonebook --entity-id="['Renuka,Apte']"
{% endhighlight %}
</div>

    Looking up entity: ['Renuka,Apte'] from kiji table: kiji://localhost:2181/default/phonebook/
    entity-id=['Renuka,Apte'] [1384235579766] info:firstname
                                     Renuka
    entity-id=['Renuka,Apte'] [1384235579766] info:lastname
                                     Apte
    entity-id=['Renuka,Apte'] [1384235579766] info:email
                                     ra@wibidata.com
    entity-id=['Renuka,Apte'] [1384235579766] info:telephone
                                     415-111-2222
    entity-id=['Renuka,Apte'] [1384235579766] info:address
                                     {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}

Next, to perform the deletion of this contact using DeleteEntry:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-{{site.phonebook_devel_version}}.jar \
    org.kiji.examples.phonebook.DeleteEntry
{% endhighlight %}
</div>

    First name: Renuka
    Last name: Apte

#### Verify
To verify that the row has been deleted, run the following command ensuring that the phonebook
entry for Renuka does not get printed:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji get ${KIJI}/phonebook --entity-id="['Renuka,Apte']"
{% endhighlight %}
</div>

    Looking up entity: ['Renuka,Apte'] from kiji table: kiji://localhost:2181/default/phonebook/

## Wrapping up
If you started your BentoBox to do this tutorial, now would be a good time to stop it.

<div class="userinput">
{% highlight bash %}
bento stop
{% endhighlight %}
</div>

To learn more about Kiji, check out these other resources:
 - [User Guide]({{site.userguide_schema_devel}}/kiji-schema-overview)
 - [API Docs](http://docs.kiji.org/apidocs)
 - [Source Code](http://github.com/kijiproject)

For information about the Kiji Project and user-to-user support:
<a class="btn btn-primary" href="mailto:user+subscribe@kiji.org">Sign up for user@kiji.org</a>

Hungry for more? To learn about KijiMR, Kiji's MapReduce integration library,
check out the
[Music recommendation tutorial]({{site.tutorial_music_devel}}/music-overview/).
