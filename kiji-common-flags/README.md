kiji-common-flags
=================

A simple flag parsing library with an API that works well for the common case.


### Example usage

    import org.kiji.common.flags.*;

    public class MyApp {
      @Flag(usage="enable the foo feature")
      public boolean foo = false;

      @Flag(name="server", usage="server address")
      public String serverHostPort = "localhost:8123";

      public static void main(String[] args) {
        MyApp myApp = new MyApp();
        List<String> nonFlagArgs = FlagParser.init(myApp, args);

        // At this point myApp.foo and myApp.serverHostPort have been assigned.
      }
    }


### Notes

* Flags look like `--name=value` or `-name=value`.
* Boolean flags may have no value, e.g., `--name` instead of `--name=true`.
* Any flags after ` -- ` are ignored.
* If there are duplicate flags, it keeps the last one.
* A flag named `--help` prints usage info (unless you declare the flag yourself).


### How to build

To build `target/kiji-common-flags-${project.version}.jar` run:

    mvn package


