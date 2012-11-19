---
layout: post
title: Delete Contacts
category: tutorial
tags: [article]
order: 8
description: Examples of Point deletions.
---

Deletions of Kiji table cells can be performed both within a MapReduce job and from
non-distributed java programs. Both types of programs use `KijiTableWriter`s to
delete data.

## Point Deletions

You realize one of your frenemies, Renuka Apte (any resemblance to real persons, living or dead,
is purely coincidental), has somehow made it to your contact list. To remedy this we will
perform a point deletion on the row with Renuka's contact information. To permit deletions
from the phonebook, we will create a tool that will let us specify contacts that we want
to delete.

### DeleteEntry.java

DeleteEntry uses a `KijiTableWriter` to perform point deletions on a kiji table:

{% highlight java %}
// Open a table writer.
KijiTableWriter writer = table.openTableWriter();
{% endhighlight %}

The deletion is then performed by specifying the row ID for the entry, in this case
a string of the format `firstname,lastname`:

{% highlight java %}
// Create a row ID with the first and last name.
EntityId user = table.getEntityId(first + "," + last);

// Attempt to delete row for the specified user.
writer.deleteRow(user);
{% endhighlight %}

### Running the Example

This example interactively queries the user for the first and last names of the contact
to delete. First, verify that the contact entry for Renuka Apte exists in your phonebook
table:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls --table=phonebook --entity-id="Renuka,Apte"
{% endhighlight %}
</div>

    \x17\xDC\xE7\x85\x0F{\xB6SF\x9A\xB5&\xA5\x8C\x81[ [1352864000121] info:firstname
                                     Renuka
    \x17\xDC\xE7\x85\x0F{\xB6SF\x9A\xB5&\xA5\x8C\x81[ [1352864000121] info:lastname
                                     Apte
    \x17\xDC\xE7\x85\x0F{\xB6SF\x9A\xB5&\xA5\x8C\x81[ [1352864000121] info:email
                                     ra@wibidata.com
    \x17\xDC\xE7\x85\x0F{\xB6SF\x9A\xB5&\xA5\x8C\x81[ [1352864000121] info:telephone
                                     415-111-2222
    \x17\xDC\xE7\x85\x0F{\xB6SF\x9A\xB5&\xA5\x8C\x81[ [1352864000121] info:address
                                     {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}

Next, to perform the deletion of this contact using DeleteEntry:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc1.jar \
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
$KIJI_HOME/bin/kiji ls --table=phonebook --entity-id="Renuka,Apte"
{% endhighlight %}
</div>

    Looking up entity: \x17\xDC\xE7\x85\x0F{\xB6SF\x9A\xB5&\xA5\x8C\x81[ from kiji table: kiji://localhost:2181/default/phonebook/

## Deleting from a MapReduce Job

You’re tired of all your San Francisco friends bragging about their startups.
You’ve decided to clean your phonebook of anyone from the state of California. Since
you have so many contacts, it would take too long to use the point deletion tool
we created in the previous example. Instead, we will write a MapReduce job to
sanitize your phonebook of any California contacts.

### DeleteEntriesByState.java

Deletions from within a MapReduce job are performed using a `ContextKijiTableWriter`.
The DeleteEntriesByState example runs a MapReduce job that reads through the contacts
in the phonebook table and deletes any entry that has an address from the specified
state.

First, the contact's address is extracted from the row:

{% highlight java %}
public void map(EntityId entityId, KijiRowData row, Context context)
    throws IOException, InterruptedException {
  if (!row.containsColumn(Fields.INFO_FAMILY, Fields.ADDRESS)) {
    // Ignore the row if there is no address.
    return;
  }

  final String victimState = context.getConfiguration().get(CONF_STATE, "");
  final Address address = row.getValue(Fields.INFO_FAMILY, Fields.ADDRESS, Address.class);
{% endhighlight %}

A `ContextKijiTableWriter` is then used to delete the row if the state matches:

{% highlight java %}
  if (victimState.equals(address.getState().toString())) {
    // Delete the entry.
    final ContextKijiTableWriter writer = new ContextKijiTableWriter(context);
    try {
      writer.deleteRow(entityId);
    } finally {
      writer.close();
    }
  }
}
{% endhighlight %}

### Running the Example

You can run the DeleteEntriesByState MapReduce job by running:

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji jar \
    $KIJI_HOME/examples/phonebook/lib/kiji-phonebook-1.0.0-rc1.jar \
    org.kiji.examples.phonebook.DeleteEntriesByState --state=CA
{% endhighlight %}
</div>

#### Verify
To verify that the contacts have been deleted, run the following command ensuring that no
phonebook entries from California get printed.

<div class="userinput">
{% highlight bash %}
$KIJI_HOME/bin/kiji ls --table=phonebook --columns="derived:state"
{% endhighlight %}
</div>

    Scanning kiji table: kiji://localhost:2181/default/phonebook/
    U\x1EP\xC1\xF2c$7\xCC\xBA\xCB\x16\x10\x0F\x11\xDB [1352831738599] derived:state
                                 DC

## Wrapping up
If you started your BentoBox to do this tutorial, now would be a good time to stop it.

<div class="userinput">
{% highlight bash %}
bento stop
{% endhighlight %}
</div>

To learn more about Kiji, check out these other resources:
 - [User Guide]({{site.userguide_url}}kiji-schema-overview)
 - [API Docs](http://docs.kiji.org/apidocs)
 - [Source Code](http://github.com/kijiproject)

For information about the Kiji Project and user-to-user support:
<a class="btn btn-primary" href="mailto:user+subscribe@kiji.org">Sign up for user@kiji.org</a>
