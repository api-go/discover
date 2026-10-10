require 'redis'
require 'net/http'
require 'json'
require 'securerandom'
require 'logger'
require 'uri'
require 'monitor'

module ApigoDiscover
  HEALTH_PATH = '/_discover/health'.freeze
  RELAY = %w[x-real-ip x-forwarded-for x-user-id x-device-id x-client-app-name x-client-app-version x-session-id x-request-id x-host x-scheme x-user-agent].freeze
  @config = {registry:'redis://127.0.0.1:6379/15',app:'',weight:100,calls:{},callRetryTimes:10}
  @lock = Monitor.new
  @lifecycle = Monitor.new
  @nodes, @calls, @threads, @subscribers = {}, {}, {}, {}
  @running = false
  @logger = Logger.new($stderr)
  class << self
    attr_reader :config
    def healthPath; HEALTH_PATH; end
    def init
      return if @inited
      {registry:'REGISTRY',app:'APP',weight:'WEIGHT',calls:'CALLS'}.each do |key,name|
        value=ENV['DISCOVER_'+name]; next unless value
        @config[key]=key==:calls ? JSON.parse(value) : key==:weight ? Integer(value) : value
      end
      @inited=true
    end
    def configure(value); init; @config.merge!(value); end
    def parse(value)
      result={timeout:10.0,httpVersion:2,ssl:false,token:''}
      return result.merge(value) if value.is_a?(Hash)
      value.to_s.split(':').each do |part|
        case part
        when '1','2'; result[:httpVersion]=part.to_i
        when 'https','s'; result.merge!(ssl:true,httpVersion:2)
        when 'http','h2c'; result.merge!(ssl:false,httpVersion:part=='http' ? 1 : 2)
        else
          if part.match?(/^\d+(ms|s)?$/); result[:timeout]=part.to_f*(part.end_with?('ms') ? 0.001 : 1)
          elsif !part.empty?; result[:token]=part
          end
        end
      end
      result
    end
    def setNode(app,addr,weight)
      @lock.synchronize do
        values=(@nodes[app]||={})
        if weight<=0; values.delete(addr)
        elsif values[addr]; values[addr][:weight]=weight
        else; values[addr]={addr:addr,weight:weight,usedTimes:0,failedTimes:0}
        end
      end
    end
    def getAppNodes(app); @lock.synchronize {(@nodes[app]||{}).transform_values(&:dup)}; end
    def fetchApp(app)
      return unless @pool
      live=@pool.hgetall(app).select {|addr,_| @pool.exists?(app+'_'+addr)}
      @lock.synchronize do
        (@nodes[app]||{}).keys.each {|addr| setNode(app,addr,0) unless live.key?(addr)}
        live.each {|addr,weight| setNode(app,addr,weight.to_i)}
      end
    end
    def addExternalApp(app,callConf='')
      @lock.synchronize do
        @calls[app]=parse(callConf);@nodes[app]||={}
        if @running && !@threads[app]
          @threads[app]=Thread.new do
            while @running
              begin
                subscriber=Redis.new(url:@config[:registry],timeout:2,reconnect_attempts:0)
                @subscribers[app]=subscriber
                subscriber.subscribe('CH_'+app) do |on|
                  on.subscribe {|_,_| fetchApp(app)}
                  on.message do |_,message|
                    parts=message.split(' ');setNode(app,parts[0],Integer(parts[1])) if parts.size==2
                  end
                end
              rescue StandardError=>error
                @logger.warn("discover subscription: #{error}") if @running
                @lock.synchronize {@condition.wait(0.25)} if @running
              ensure
                subscriber.close if subscriber
              end
            end
          end
        end
      end
      fetchApp(app);true
    end
    def register
      return unless isServer
      app,addr,weight=@config[:app],@addr,@config[:weight]
      unless @pool.hexists(app,addr)
        @pool.hset(app,addr,weight);@pool.setex(app+'_'+addr,10,'1');@pool.publish('CH_'+app,"#{addr} #{weight}")
      else; @pool.setex(app+'_'+addr,10,'1')
      end
    end
    def start(addr='')
      @lifecycle.synchronize do
        init; stop; @addr=addr;@pool=Redis.new(url:@config[:registry],timeout:2);@pool.ping;@running=true;@condition=@lock.new_cond
        register
        if isServer
          @threads[:heartbeat]=Thread.new do
            while @running
              @lock.synchronize {@condition.wait(1)}
              begin;register if @running;rescue StandardError=>error;@logger.warn(error);end
            end
          end
        end
        @config[:calls].each {|app,conf| addExternalApp(app,conf)};true
      end
    end
    def stop
      @lifecycle.synchronize do
        server=isServer;@running=false;@lock.synchronize {@condition.broadcast if @condition}
        @subscribers.each_value(&:close);@subscribers.clear
        @threads.each_value {|thread| thread.join(3)};@threads.clear
        if @pool
          if server
            app=@config[:app];@pool.hdel(app,@addr);@pool.del(app+'_'+@addr);@pool.publish('CH_'+app,"#{@addr} 0")
          end
          @pool.close;@pool=nil
        end
        @addr=''
      end
    end
    def wait; @threads.values.each(&:join); end
    def isServer; !!(@running && @addr && !@addr.empty? && !@config[:app].empty? && @config[:weight]>0); end
    def isClient; @running; end
    def setLogger(value); @logger=value; end
    def setRoute(value); @route=value; end
    def setLoadBalancer(value); @lb=value; end
    def setHealthHandler(value); @health=value; end
    def healthHandler(request={})
      return [405,{'Allow'=>'GET'},[]] if request.fetch(:method,'GET')!='GET'
      @health ? @health.call(request) : [200,{'Content-Type'=>'text/plain; charset=utf-8'},['OK']]
    end
    def middleware(app)
      lambda do |env|
        if env['PATH_INFO']==HEALTH_PATH
          healthHandler(method:env['REQUEST_METHOD'],env:env)
        else
          env['HTTP_X_REQUEST_ID']||=SecureRandom.hex(16)
          previous=Thread.current[:discoverRequest];Thread.current[:discoverRequest]=fromRack(env)
          begin;app.call(env);ensure;Thread.current[:discoverRequest]=previous;end
        end
      end
    end
    def fromRack(env)
      {rackEnv:env,headers:env.select {|k,_| k.start_with?('HTTP_')}.transform_keys {|k| k[5..-1].tr('_','-')},clientIP:env['REMOTE_ADDR'],host:env['HTTP_HOST'],scheme:env['rack.url_scheme'],userAgent:env['HTTP_USER_AGENT']}
    end
    def newCaller(request=nil,logger=nil); Caller.new(request || Thread.current[:discoverRequest],logger); end
    def callInfo(app); addExternalApp(app) unless @calls[app];@calls[app];end
    def route;@route;end
    def selectNode(call,excluded,request,withNode='')
      @lock.synchronize do
        all=(@nodes[call[:app]]||{}).values.reject {|n| excluded.include?(n[:addr]) || (!withNode.empty? && withNode!=n[:addr])}
        ready=all.select {|n| n[:failedTimes]<@config[:callRetryTimes]};ready=all if ready.empty?
        raise IOError,"all nodes failed for #{call[:app]}" if ready.empty?
        node=@lb ? @lb.next(call,ready,request) : ready.min_by {|n| n[:usedTimes].to_f/n[:weight]}
        node[:usedTimes]+=1;node
      end
    end
    def finish(call,node,error,response)
      failed=!!error || (response && (502..504).include?(response.code.to_i))
      count=@lock.synchronize {node[:failedTimes]=failed ? node[:failedTimes]+1 : 0}
      @lb.response(call,node,error,response) if @lb
      if failed && count>=@config[:callRetryTimes] && @pool
        @pool.hdel(call[:app],node[:addr]);@pool.publish('CH_'+call[:app],"#{node[:addr]} 0")
      end
      failed
    end
    def source; {'x-from-app'=>@config[:app],'x-from-node'=>@addr}; end
  end
  class Caller
    def initialize(request=nil,logger=nil);@request=request||{};@logger=logger;end
    def doRequest(method,app,path,data=nil,headers={},withNode='')
      request=@request[:rackEnv] ? ApigoDiscover.fromRack(@request[:rackEnv]) : @request
      call={method:method,app:app,path:path,data:data,headers:headers};ApigoDiscover.route.call(call,request) if ApigoDiscover.route
      info=ApigoDiscover.callInfo(call[:app]);raise ArgumentError,'Ruby native HTTP does not support HTTP/2; set call config to 1/http' if info[:httpVersion]==2
      raise ArgumentError,'path must be local absolute' unless call[:path].start_with?('/') && !call[:path].start_with?('//')
      hs={};(request[:headers]||{}).each {|name,value| key=name.downcase;hs[key]=Array(value).dup if RELAY.include?(key)||key.start_with?('x-app-')}
      {'x-real-ip'=>request[:clientIP],'x-host'=>request[:host],'x-scheme'=>request[:scheme],'x-user-agent'=>request[:userAgent],'x-request-id'=>SecureRandom.hex(16)}.each {|key,value| hs[key]||=[value] if value}
      call[:headers].each {|key,value| hs[key.downcase]=Array(value)};ApigoDiscover.source.each {|key,value| hs[key]=[value]} if ApigoDiscover.isServer
      hs['access-token']=[info[:token]] unless info[:token].empty?
      body=call[:data];if body.is_a?(Hash)||body.is_a?(Array);body=JSON.generate(body);hs['content-type']||=['application/json'];end
      raise ArgumentError,'body must be replayable' if body && !body.is_a?(String)
      excluded=[];deadline=Process.clock_gettime(Process::CLOCK_MONOTONIC)+info[:timeout]
      loop do
        remaining=deadline-Process.clock_gettime(Process::CLOCK_MONOTONIC);raise Timeout::Error,'discover total timeout' if remaining<=0
        node=ApigoDiscover.selectNode(call,excluded,request,withNode);excluded<<node[:addr]
        uri=URI((info[:ssl] ? 'https' : 'http')+'://'+node[:addr]+call[:path]);outbound=Net::HTTPGenericRequest.new(call[:method],!body.nil?,call[:method]!='HEAD',uri.request_uri)
        hs.each {|key,values| values.each {|value| outbound.add_field(key,value)}};outbound.body=body
        response=nil;error=nil
        begin
          response=Net::HTTP.start(uri.hostname,uri.port,use_ssl:info[:ssl],open_timeout:remaining,read_timeout:remaining,write_timeout:remaining) {|client| client.request(outbound)}
        rescue IOError,SystemCallError,Timeout::Error,OpenSSL::SSL::SSLError=>error
        end
        return {statusCode:response.code.to_i,headers:response.to_hash,body:response.body,node:node[:addr],native:response} unless ApigoDiscover.finish(call,node,error,response)
      end
    end
    define_method(:do) {|*args| doRequest(*args)}
    def open(app,path,headers={})
      require 'websocket-client-simple'
      raise ArgumentError,'path must be local absolute' unless path.start_with?('/') && !path.start_with?('//')
      request=@request[:rackEnv] ? ApigoDiscover.fromRack(@request[:rackEnv]) : @request
      info=ApigoDiscover.callInfo(app);raise NotImplementedError,'WSS requires a transport with hostname verification; native adapter supports WS only' if info[:ssl];call={method:'WS',app:app,path:path};excluded=[];deadline=Process.clock_gettime(Process::CLOCK_MONOTONIC)+info[:timeout]
      hs={};(request[:headers]||{}).each {|name,values| key=name.downcase;hs[key]=Array(values).join(', ') if RELAY.include?(key)||key.start_with?('x-app-')}
      hs['x-request-id']||=SecureRandom.hex(16);hs.merge!(headers);hs.merge!(ApigoDiscover.source) if ApigoDiscover.isServer;hs['access-token']=info[:token] unless info[:token].empty?
      loop do
        node=ApigoDiscover.selectNode(call,excluded,request);excluded<<node[:addr];socket=nil;error=nil
        begin
          events=Queue.new
          socket=WebSocket::Client::Simple.connect((info[:ssl] ? 'wss' : 'ws')+'://'+node[:addr]+path,headers:hs) do |ws|
            ws.on(:open) {events<<[:open,nil]};ws.on(:error) {|e| events<<[:error,e]}
          end
          remaining=deadline-Process.clock_gettime(Process::CLOCK_MONOTONIC);raise Timeout::Error if remaining<=0
          state,value=Timeout.timeout(remaining) {events.pop};raise(value.is_a?(Exception) ? value : IOError.new(value.to_s)) if state==:error
          ApigoDiscover.finish(call,node,nil,nil);return socket
        rescue IOError,SystemCallError,Timeout::Error=>error
          socket.close if socket;ApigoDiscover.finish(call,node,error,nil)
        end
      end
    end
    def get(app,path,headers={});doRequest('GET',app,path,nil,headers);end
    def post(app,path,data=nil,headers={});doRequest('POST',app,path,data,headers);end
    def put(app,path,data=nil,headers={});doRequest('PUT',app,path,data,headers);end
    def delete(app,path,data=nil,headers={});doRequest('DELETE',app,path,data,headers);end
    def head(app,path,headers={});doRequest('HEAD',app,path,nil,headers);end
    def doWithNode(method,app,withNode,path,data=nil,headers={});doRequest(method,app,path,data,headers,withNode);end
  end
end
