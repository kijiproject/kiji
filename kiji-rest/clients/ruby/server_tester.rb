require "./lib/kijirest/server.rb"
#require 'kijirest/server'

s=KijiRest::Server.new("/Users/amit/Documents/workspace/kiji-rest/kiji-rest/target/kiji-rest-0.1.0-SNAPSHOT-release/kiji-rest-0.1.0-SNAPSHOT")
s.start("(xwing07.ul.wibidata.net,xwing09.ul.wibidata.net,xwing11.ul.wibidata.net):2181",["default","retail"], true)
c=s.new_client
puts c.instances.inspect
puts c.tables("retail").inspect
s.stop(true)

