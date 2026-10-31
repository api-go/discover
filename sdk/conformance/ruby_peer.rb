require_relative '../ruby/lib/apigo_discover'
app=ENV.fetch('DISCOVER_TEST_PEER');addr=ENV.fetch('DISCOVER_TEST_ADDR');target=ENV.fetch('DISCOVER_TEST_TARGET')
ApigoDiscover.configure(app:app,registry:'redis://127.0.0.1:6379/15',calls:{target=>'1'})
begin
 ApigoDiscover.start(addr)
 deadline=Process.clock_gettime(Process::CLOCK_MONOTONIC)+8
 while Process.clock_gettime(Process::CLOCK_MONOTONIC)<deadline && (!ApigoDiscover.isRegistered || ApigoDiscover.getAppNodes(target).empty?)
  IO.select(nil,nil,nil,0.01)
 end
 raise "discovery not ready" unless ApigoDiscover.isRegistered && !ApigoDiscover.getAppNodes(target).empty?
 response=ApigoDiscover.newCaller(headers:{'X-App-Labels'=>['a','b']},clientIP:'192.0.2.4',userAgent:'conformance').post(target,'/echo','payload')
 data=JSON.parse(response[:body]);raise 'body mismatch' unless data['method']=='POST'&&data['body']=='payload'
 headers=data['headers'].transform_keys(&:downcase);raise 'source mismatch' unless headers['x-from-app']==[app]
 raise 'multi-value mismatch' unless headers['x-app-labels'].join(', ')=='a, b'
 raise 'health mismatch' unless ApigoDiscover.healthHandler==[200,{'Content-Type'=>'text/plain; charset=utf-8'},['OK']]
 ApigoDiscover.setHealthHandler(->(_) {[503,{},['draining']]});raise 'custom health mismatch' unless ApigoDiscover.healthHandler[0]==503
 queue=Queue.new;ws=ApigoDiscover.newCaller.open(target,'/ws');ws.on(:message) {|message| queue<<message.data};ws.send('hello');raise 'websocket echo mismatch' unless Timeout.timeout(2) {queue.pop}=='hello';ws.close
ensure
 ApigoDiscover.stop
end
puts 'PASS Ruby -> Go HTTP'
