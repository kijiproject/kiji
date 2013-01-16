Kiji Annotations
================

This module contains Java @annotations that specify various aspects of the
system's overall behavior. To start, we have created API audience and
stability level annotations which will appear in the Javadoc. Developers
should consult these and only depend on APIs that meet their intended audience
level (public, framework, etc) or their tolerance for stability between
versions (stable, evolving, or unstable).

Over time, this module may contain additional annotations that specify other
aspects of our expected system semantics.

For more information, see the Javadoc comments for our existing annotations:

* [API audience
  annotations](https://github.com/kijiproject/annotations/blob/master/src/main/java/org/kiji/annotations/ApiAudience.java)
* [API stability
annotations](https://github.com/kijiproject/annotations/blob/master/src/main/java/org/kiji/annotations/ApiStability.java)
* [API inheritance
  annotations](https://github.com/kijiproject/annotations/blob/master/src/main/java/org/kiji/annotations/Inheritance.java)
