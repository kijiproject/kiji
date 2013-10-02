# (c) Copyright 2013 WibiData, Inc.
#
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "kijirest/version"
require "kijirest/client"
require 'yaml'
require 'open-uri'

module KijiRest

  # Class that wraps the starting/stopping of a KijiREST server.
  # assumes that this is running as a non-privileged user that
  # has permissions to read/write to the location where KIJI_REST is
  # installed
  class Server
    KIJI_REST_HOME = "/opt/wibi/kiji-rest"
    def initialize(kiji_rest_home = KIJI_REST_HOME)
      @kiji_server_location = kiji_rest_home
      unless authorized_to_run?
        raise "#{ENV['USER']} not authorized to run KijiREST server!"
      end
      @http_port = 8080
      reload_configuration!
    end

    # Start a server given a zookeeper quorum and array of visible
    # instances. This will modify the configuration.yml and launch the server.
    # param: zk_quorum is the list of zookeeper servers. Default is .env
    # param: visible_instances is an array of instance names that are to be visible by clients
    #        of the REST server.
    # param: wait_for_load specifies whether or not to block until the server has come up. If the
    #        server fails to come up after some period of time, an exception will be raised.
    def start(zk_quorum = ".env", visible_instances = ["default"], wait_for_load = false)
      if running?
        raise "KijiREST appears to be running."
      else
        #Update the configuration file to reflect the desired options
        dropwizard_config = YAML.load_file(qualify_file("/conf/configuration.yml"))

        #Do a bit of string checking so that we aren't adding kiji:// prefix twice.
        prefix="kiji://"
        prefix = "" if zk_quorum[0..6] == "kiji://"

        kiji_uri = "#{prefix}#{zk_quorum}"
        dropwizard_config["cluster"] = kiji_uri
        dropwizard_config["instances"] = visible_instances
        f = File.new(qualify_file("/conf/configuration.yml"), "w")
        f.puts(dropwizard_config.to_yaml)
        f.close
        #Now start the service
        launch_command = qualify_file("/bin/kiji-rest")
        %x{#{launch_command}}
        if wait_for_load
          (1..20).each {|i|
            break if app_running?
            sleep 2
            }
          unless app_running?
            raise "App failed to start!"
          end
        end
      end
    end

    # Stops the server optionally waiting for the server to shutdown
    # param: wait_for_shutdown determines whether or not to wait for the server to have shutdown
    #        before returning.
    def stop(wait_for_shutdown = false)
      pid_file = qualify_file("kiji-rest.pid")
      if File.exist?(pid_file)
        pid_file_obj = File.new(pid_file, "r")
        pid = pid_file_obj.gets
        pid_file_obj.close
        #Kill the pid cleanly first
        %x{kill #{pid}}
        if wait_for_shutdown
          (1..20).each {|i|
            break unless app_running?
            sleep 2
            }
           if app_running?
             %x{kill -9 #{pid}}
           end
        end
        File.delete(pid_file)
      else
        raise "KijiREST not running!"
      end
    end

    # Returns a new client instance.
    def new_client
      Client.new("http://localhost:#{@http_port}")
    end

    private

    # Reloads the configuration.yml storing the configuration in a private member
    # variable.
    def reload_configuration!
      @dropwizard_config = YAML.load_file(qualify_file("/conf/configuration.yml"))
      @http_port = 8080
      if @dropwizard_config.include?("http") && @dropwizard_config["http"].include?("port")
          @http_port = @dropwizard_config["http"]["port"]
      end
    end

    # Checks if this current user is authorized to run the server. A simple check
    # is to ensure that the current user is the owner of a known file (conf/configuration.yml)
    def authorized_to_run?
      File.owned?(qualify_file("/conf/configuration.yml"))
    end

    # Checks if the application is running by hitting a known URI
    def app_running?
      begin
        f = open("http://localhost:#{@http_port}/v1/instances")
        f.close
        true
      rescue
        false
      end
    end

    # Checks if the pid file exists. This will check for the pid file generated
    # when the server is launched by a non-privileged user as well as the pid file
    # generated when this server is launched by /sbin/service.
    def pid_exists?
      local_pid_file = qualify_file("kiji-rest.pid")
      global_pid_file = "/var/run/kiji-rest/kiji-rest.pid"
      File.exists?(local_pid_file) || File.exists?(global_pid_file)
    end

    # Checks if the server is running by checking the process file (kiji-rest.pid)
    # or the global/root run /var/run/kiji-rest.pid
    def running?
      app_running? || pid_exists?
    end

    # Returns a fully qualified filename prefixed by the location of kiji_rest_server
    def qualify_file(filename)
      "#{@kiji_server_location}/#{filename}"
    end
  end
end
