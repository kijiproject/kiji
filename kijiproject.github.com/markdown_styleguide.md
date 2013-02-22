This is meant to be a quick reference for how we use Markdown.
This cheat sheet is mostly cribbed from the excellent markdown
documentation at http://daringfireball.net/projects/markdown/.

Emphasis
--------
  You can make italics *this way* or bold it **this way**.

Links
-----
  Links can be made to the absolute path of a page like this:
    [text for link](http://url.com/ "Title")

  Links various versions of the userguides and api docs are made more convenient via:
    [text for link]({{userguide_schema_rc4}}title-of-page) or
    [`text for link`]({{site.api_url}}classname.html)
  note that in _config.yml. we have the following variables defined:
production_url : http://docs.kiji.org
userguide_url : /userguides
userguide_schema_rc4 : /userguides/schema/1.0.0-rc4
userguide_mapreduce_rc4 : /userguides/mapreduce/1.0.0-rc4

tutorial_url : /tutorials
api_url : /apidocs
api_schema_rc4 : /apidocs/kiji-schema/1.0.0-rc4/org/kiji/schema
api_mr_rc4 : /apidocs/kiji-mapreduce/1.0.0-rc4/org/kiji/mapreduce
api_mrlib_rc4 : /apidocs/kiji-mapreduce-lib/1.0.0-rc4/org/kiji/mapreduce

  So, for example, instead of
    [Managing Data](http://docs.kiji.org/userguide/schema/1.0.0-rc4/managing-data)
  you could write
    [Managing Data]({{site.userguide_schema_rc4}}managing-data)

Code
----
  Code should by highlighted using the following template:
    {% highlight lang_name %}
      code goes here
    {% endhighlight %}
  Where lang_name is java, js, bash, xml or any other valid language from [this
  list](http://pygments.org/languages/).

Headers
-------
  We prefer this style of marking headers:
    # Header 1

    ## Header 2

    ###### Header 6

Lists
-----
  Ordered, without paragraphs:
    1.  Foo
    2.  Bar
  Unordered, with paragraphs:
    *   A list item.
  With multiple paragraphs.
    *   Bar
      You can nest them:
    *   Abacus
        * answer
    *   Bubbles
        1.  bunk
        2.  bupkis
            * BELITTLER
        3. burper
    *   Cunning

Blockquotes
-----------
> Email-style angle brackets
> are used for blockquotes.

> > And, they can be nested.

> #### Headers in blockquotes
> 
> * You can quote a list.
> * Etc.
