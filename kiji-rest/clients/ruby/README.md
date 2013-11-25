# kijirest-client gem

This gem provides wrapper classes around accessing and managing a KijiREST server. There
are two modules:

*  KijiRest::Client - Provides a client interface for accessing KijiREST
*  KijiRest::Server - Provides a server side interface for starting/stopping a local KijiREST
   server assuming that the user running this code is the same user who can start/stop the server.

For more information about the KijiREST project, please visit
[https://github.com/kijiproject/kiji-rest](https://github.com/kijiproject/kiji-rest)

# Installation

Add this line to your application's Gemfile:

    gem 'kijirest-client'

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install kijirest-client

# Usage

## KijiRest::Client

    require 'kijirest/client'

    # Construct a new client pointing at localhost:8080
    c=KijiRest::Client.new("http://localhost:8080")

    # List instances
    instances = c.instances

    # List tables for a known instance ("default")
    tables = c.tables("default") # Assuming the layout created from http://www.kiji.org/getstarted/#Quick_Start_Guide

    # Create a new user
    eid = ["my_user"]
    row_hash={
      KijiRest::Client::ENTITY_ID => eid,
      "cells" => {
        "info" => {
          "email" => [ {
            "value" => "name@company.com",
            "writer_schema" => "string"
            }
          ]
        }
      }
    }

    # Write the new row. The final argument is what dictates whether or not to strip
    # timestamps from an existing row_hash or not. This is handy if you are using the results
    # from the GET of another row to write a new row.
    puts c.write_row("default","users",row_hash,true)

    # Verify that what we wrote is what we expected
    my_written_row = c.row("default","users",["my_user"])
    puts my_written_row["cells"]["info"]["email"][0]["value"] == "name@company.com"
## KijiRest::Server

    require 'kijirest/server'

    # Construct a new server object passing in the location where the kiji-rest application
    # resides. Note: The owner of the kiji-rest folder must be the same user who is running
    # this code else an exception will be raised.
    s = KijiRest::Server.new("/path/to/kiji-rest")

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
