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
jekyll-bootstrap. Learn more about jekyll-bootstrap at
http://www.jekyllbootstrap.com

## Creating and editing documentation
To add a new file, find the user guide section or article you want and
create a file named YYYY-MM-DD-title.md under the _posts directory,
and write it using markdown syntax. In the file, you should include
the following at the top of the file (include the dashes):

    ---
    layout: post
    title: My Content Title
    category: [userguide, schema, 1.0.0-rc1] or tutorial
    tags: [doc_type]
    description: A tutorial on computer stuff.
    ---

The above is YAML Front Matter syntax that instructs Jekyll what to do
with the file when compiling it into a static site. Set the 'doc_type'
is either 'article' or 'schema_ug'. The tag allows us to collate
articles and userguides. The ordering of these articles and userguides
is determined by the date in the filename. Janky, c'est la vie.
You can write everything in markdown (see markdown_styleguide.md for more
information.) and do code highlighting inline with backticks `code` or
in blocks with the template:

    {% highlight java %}
    block of code goes here
    {% endhighlight %}

Java in the above template can be any short name for a language from
[this list.](http://pygments.org/languages/) 

## Previewing Changes

In order to preview what your changes look like, you will need to have
Ruby and Jekyll installed. It is highly recommended that you control your
ruby version using rvm. Check out instructions at
http://github.com/mojombo/jekyll. Once Jekyll is installed `jekyll
--server --safe` will display the site corresponding to the
current state of the project at http://localhost:4000. Note that the
pygments highlighting of codeblocks will only work if you have
pygments installed.
