require 'webrick'
require_relative '../ruby/lib/apigo_discover'
server=WEBrick::HTTPServer.new(BindAddress:'127.0.0.1',Port:0,Logger:WEBrick::Log.new($stderr,WEBrick::Log::ERROR),AccessLog:[])
app=ApigoDiscover.middleware(->(_) {[404,{},[]]})
server.mount_proc('/') do |request,response|
 code,headers,body=app.call({'PATH_INFO'=>request.path,'REQUEST_METHOD'=>request.request_method,'REMOTE_ADDR'=>request.peeraddr[3],'HTTP_HOST'=>request.host,'rack.url_scheme'=>'http'})
 response.status=code;headers.each {|key,value| response[key]=value};response.body=body.to_a.join
end
addr="127.0.0.1:#{server.listeners.first.addr[1]}";ApigoDiscover.configure(app:ENV.fetch('DISCOVER_TEST_PEER'),registry:'redis://127.0.0.1:6379/15',calls:{});ApigoDiscover.start(addr)
trap('TERM') {server.shutdown}
puts 'DISCOVER_READY '+addr;$stdout.flush
begin;server.start;ensure;ApigoDiscover.stop;end
