(c) Copyright 2012 WibiData, Inc.

kiji-docs
=========

This repository contains documentation for the [KijiProject](http://www.kiji.org).
This documentation is compiled into a static website using configuration
files Jekyll.

## How words become websites

Our docs will be transformed into readable pages using Jekyll. Jekyll
allows us to write our documentation in markdown, and host it as
static pages in github. We are particularly using
jekyll-bootstrap. [Learn more about jekyll-bootstrap.](http://www.jekyllbootstrap.com)


## Creating and editing documentation
You can write everything in markdown (see markdown_styleguide.md for more
information.) and do code highlighting inline with backticks `code` or
in blocks with the template:

    {% highlight java %}
    block of code goes here
    {% endhighlight %}

Java in the above template can be any short name for a language from
[this list.](http://pygments.org/languages/) 

To add a new file, find the user guide section or article you want and
create a file named YYYY-MM-DD-title.md under the _posts directory,
and write it using markdown syntax. In the file, you should include
the following at the top of the file (include the dashes):

    ---
    layout: post
    title: Delete Contacts
    categories: [tutorials, phonebook-tutorial, 1.0.0-rc4]
    tags: [article]
    order: 8
    description: Examples of Point deletions.
    ---

The above is YAML Front Matter syntax that instructs Jekyll what to do
with the file when compiling it into a static site. The tag allows us to collate
articles and userguides.


## Previewing Changes
### PreReqs
In order to preview what your changes look like, you will need to have
Ruby and Jekyll installed. It is highly recommended that you control your
ruby version using rvm. Check out instructions at
http://github.com/mojombo/jekyll.

### Seeing Something
To see how your local version of kiji-docs renders, run `rake preview`. This
command turns off google analytics, so as not to inflate our stats, and runs 
`jekyll --no-auto --server --pygments --no-lsi --safe`. Since we use the no-auto
parameter, you will need to rerun preview to see new changes, but trust us,
it is better this way.

Note that the pygments highlighting of codeblocks will only work if you have
pygments installed.

## Contributing Documentation

* Fork the [documentation project](https://github.com/kijiproject/kijiproject.github.com).
* File a JIRA for the change you want to make on the [DOCS project](http://jira.kiji.org).
* Create a topic branch: git checkout -b my_fix.
* Refer to markdown_styleguide.md in the parent directory for more on the syntax.
* Make your changes.
* Reference the jira in the commit message (e.g., "DOCS-1: Subscribe buttons to the mailing lists on the website are broken")
* Push your branch: git push origin my_fix.
* Use [reviewboard](http://review.kiji.org) to contribute your changes once you are done.
