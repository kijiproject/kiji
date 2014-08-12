KijiREST Load Testing${project.version}
=======================================

Contains procedures and JMeter test plans to measure the load that KijiREST can handle.

This package contains the following directories:

* lib/ -- contains library dependencies as well as the load-testing jar.
* bin/ -- contains script to run JMeter tests.
* test-plans/ -- contains JMeter test-plans.

Performing load testing requires two machines: the server and the client.

Server setup
------------

Create an instance to house the load testing tables.

```bash
$ kiji install --kiji=loadtesting
```

Untar the load-testing tarball somewhere and generate random data.

```bash
$ tar xvzf load-test-x.y.z.tar.gz
$ cd load-test-x.y.z
$ java -cp 'lib/*' org.kiji.rest.load_test.RandomDataGenerator kiji://.env/loadtesting
```

If you are using a bento box, you may have to first set your classpath to whatever Kiji uses.  Try
these commands:

```bash
KIJI_CP=$(kiji classpath)
$ java -cp ${KIJI_CP}:'lib/*' org.kiji.rest.load_test.RandomDataGenerator kiji://.env/loadtesting
```

You may also want to turn off request logging in KijiREST.  To do so, edit your KijiREST
`conf/configuration.yml` file:

```yaml
http:
  ...
  requestLog:
    ...
    file:
      enabled: false
```

Start the kiji-rest server on the server.

Client setup
------------

Download and extract Apache JMeter 2.9.

```bash
$ wget http://archive.apache.org/dist/jmeter/binaries/apache-jmeter-2.9.tgz
$ tar -xvzf apache-jmeter-2.9.tgz
$ export JMETER_DIR=$(pwd)
```

Untar the load-testing tarball somewhere and copy the load-testing and jersey-core jars to the
jmeter `lib/ext` directory.

```bash
$ tar xvzf load-test-x.y.z.tar.gz
$ cd load-test-x.y.z
$ cp lib/load-testing-x.y.z.jar JMETER_DIR/lib/ext
$ cp lib/jersey-core-1.17.1.jar JMETER_DIR/lib/ext
```

Run jmeter tests with the provided plans using the following commands.

```bash
$ jmeter -n -t test-plans/gets-test-plan.jmx -l results_from_gets.csv -Jthreads=20 \
-Jrampup=10 -Jloop=100 -Jdomain=http://server:8080 -Jinstance=loadtesting -Jtable=users

$ jmeter -n -t test-plans/posts-test-plan.jmx -l results_from_posts.csv -Jthreads=20 \
-Jrampup=10 -Jloop=100 -Jdomain=http://server:8080 -Jinstance=loadtesting -Jtable=users
```

The parameters are as follows:

* Jthreads corresponds to the number of users accessing the REST server
* Jrampup is the rampup time in seconds after which all threads will be hitting REST.
  This allows for staggering of users as opposed to all hitting at once upon launch
  of the load test.
* Jloop is the number times each thread will send its batch of requests to the REST server.
  The results (where each line corresponds to a request) are stored in results.csv.

Running these test-plans individually is somewhat difficult, so instead run bin/test-suite.sh.

```bash
sh test-suite.sh GET http://server:8080 loadtesting users
sh test-suite.sh POST http://server:8080 loadtesting users
sh test-suite.sh FRESHEN http://server:8080 loadtesting users
```

