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

  Links to the userguide and tutorial are made more convenient via:
    [text for link]({{site.userguide_url}}title-of-page) or
    [text for link]({{site.tutorial_url}}title-of-page) or
    [`text for link`]({{site.api_url}}classname.html)
  note that in _config.yml these variables are defined as:
    userguide_url : http://docs.kiji.org/userguide/schema/1.0.0-rc1/
    tutorial_url : http://docs.kiji.org/tutorial/
    api_url : http://docs.kiji.org/apidocs/org/kiji/schema/

  So, for example, instead of
    [Managing Data](http://docs.kiji.org/userguide/schema/1.0.0-rc1/managing-data)
  you could write
    [Managing Data]({{site.userguide_url}}managing-data)

Code
----
  Code should by highlighted using the following template:
    {% highlight lang_name %}
      code goes here
    {% endhighlight %}
  Where lang_name is java, bash, xml or any other valid language from [this
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
