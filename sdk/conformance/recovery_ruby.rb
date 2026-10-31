require_relative '../ruby/lib/apigo_discover'
$stdout.sync=true
app=ENV.fetch('DISCOVER_TEST_APP');target=app+'-target'
ApigoDiscover.configure(app:app,calls:{target=>'1'},registry:ENV.fetch('DISCOVER_TEST_REGISTRY'))
before=Process.clock_gettime(Process::CLOCK_MONOTONIC);ApigoDiscover.start('127.0.0.1:31000')
puts JSON.generate(startedMs:(Process.clock_gettime(Process::CLOCK_MONOTONIC)-before)*1000)
ApigoDiscover.config[:callRetryTimes]=1;retained=nil
quit=false;Signal.trap('TERM'){quit=true}
until quit
 if retained.nil? && !ApigoDiscover.getAppNodes(target).empty?
  begin;ApigoDiscover.newCaller.get(target,'/');rescue StandardError;end
  checker=Redis.new(url:ENV.fetch('DISCOVER_TEST_REGISTRY'));retained=checker.hexists(target,'127.0.0.1:31999');checker.close
 end
 puts JSON.generate(globalMembershipRetained:retained,registered:ApigoDiscover.isRegistered,nodes:ApigoDiscover.getAppNodes(target).size,health:ApigoDiscover.healthHandler[0])
 IO.select(nil,nil,nil,0.1)
end
ApigoDiscover.stop;puts JSON.generate(stopped:true)
