require "./lib/kijirest/server.rb"
#require 'kijirest/server'

    # Construct a new server object passing in the location where the kiji-rest application
    # resides. Note: The owner of the kiji-rest folder must be the same user who is running
    # this code else an exception will be raised.
    s = KijiRest::Server.new("../../kiji-rest/target/kiji-rest-1.0.1-SNAPSHOT-release/kiji-rest-1.0.1-SNAPSHOT/")

    # Launch the KijiREST server setting the cluster to .env and the array of visible instances
    # to ["default"]. The final argument indicates to wait for the server to finish launching
    # before returning.
    s.start(".env",["default"], true)

    # Get a reference to a new client pointing at
    # http://localhost:<port set in conf/configuration.yml>
    c = s.new_client

    # Get a list of instances.
    puts c.instances

    # Let's stop the server and wait for the server to shutdown before returning.
    s.stop(true)
