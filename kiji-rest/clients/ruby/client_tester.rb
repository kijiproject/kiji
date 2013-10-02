require "./lib/kijirest/client.rb"
#require 'kijirest/client'

c=KijiRest::Client.new("http://localhost:8080")
rowkey = c.rowkey("default","users","user-41")

#new_eid=KijiRest::Client.format_entity_id("user-41")

puts rowkey.inspect
row_hash=c.row("default","users",rowkey)
writer_schema=["string","null"]
puts row_hash["cells"]["info"]["track_plays"][0]["writer_schema"]
row_hash["cells"]["info"]["track_plays"][0]["writer_schema"] = writer_schema
row_hash["cells"]["info"]["track_plays"][0]["value"]={"string" => "amit_user"}
c.write_row("default","users",row_hash,true)
