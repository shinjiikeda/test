
require 'red_storm'
require 'extractor'

class Spout < RedStorm::DSL::Spout
  on_init do
    @uri_list = [ 'http://www.yahoo.co.jp' ]
  end
  
  on_send :emit => false, :reliable => false do
    
    @uri_list.each do | uri |
      reliable_emit(uri, uri)
    end
    sleep 1
  end
  
  on_ack do |msg_id|
    log.info("success: #{msg_id}")
  end

  on_fail do |msg_id|
    log.info("failed: #{msg_id}")
  end
end

class CrawlerBolt < RedStorm::DSL::Bolt

  on_receive :anchor => false, :emit => false do |tuple|
    uri = tuple[0]
    begin
      charset = nil
      html = open(uri) do |f|
       charset = f.charset
         f.read
      end
      anchored_emit(tuple, uri, html, charset)
      ack(tuple)
    rescue => e
      fail(tuple)
    end
  end
end

class ExtractorBolt < RedStorm::DSL::Bolt
  on_init do 
    @extractor = Extractor.new(log)
  end
  
  on_receive :anchor => false, :emit => false do |tuple|
    uri = tuple[0]
    html = tuple[1]
    charset = tuple[2]
    
    log.info("start uri: #{uri}")
    
    ret = @extractor.extract(uri, html, charset)
    p ret
    if ! ret.nil? || ret.is_successs == true
      ack(tuple)
    else
      fail(tuple)
    end
    log.info("end uri: #{uri}")
  end
  
end

class Topology < RedStorm::DSL::Topology
  
  spout Spout do
    output_fields :uri
  end
  
  bolt CrawlerBolt, :parallelism => 2 do
    source Spout, :fields => [ :uri ]
    output_fields :uri, :html, :charset
  end
  
  bolt ExtractorBolt, :parallelism => 2 do
    source CrawlerBolt, :fields => [ :uri ]
  end

  configure do |env|
    debug false
    max_task_parallelism 4
    num_workers 4
    max_spout_pending 1000
  end
end

