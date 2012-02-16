hadoop-configurator
===================

This project contains a few helpful classes for working with [Apache
Hadoop](http://hadoop.apache.org) configuration.


### Example use

    public class Foo extends Configured {
      @HadoopConf(key="foo.value", usage="The foo value")
      private String fooValue = "defaultValue";

      @Override
      public void setConf(Configuration conf) {
        super.setConf(conf);
        HadoopConfigurator.configure(this);
        // Now fooValue has been populated with the value of conf.get("foo.value").
      }

      public void doStuff() {
        System.out.println("foo.value read from configuration: " + fooValue);
      }
    }


### How to build

To build `target/hadoop-configurator-${project.version}.jar` run:

    mvn package


### Documentation

* [Java API (Javadoc)](http://wibidata.github.com/hadoop-configurator/1.0.0/apidocs/)
