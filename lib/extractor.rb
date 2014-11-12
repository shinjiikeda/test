# encoding: utf-8
require 'nokogiri'
require 'open-uri'
require 'uri'
require 'json'
require 'pathname'

class Extractor
  def initialize(logger)
    @logger = logger
  end

  def extract(uri, html, charset)
    puts "uri: #{uri}"
    begin
      ret = {}
      
      doc = Nokogiri::HTML.parse(html, uri, charset)
      
      body = doc.xpath('//text()').map{ |n|
        n.content if n.content !~ /^\<!\-\-.*\-\-\>$/m
      }.join(' ').gsub(/\s+/, ' ')
      ret[:body] = body
      
      if !doc.title
        if keys.length > 0
          title = keys[0]
        end
      else
        title = doc.title
      end
      ret[:title] = title
      
      links = []
      doc.search('a').each do |link|
        l = link['href']
        if ! l.nil?
          if l =~ /^http/
            links << l
          else
            begin
              links << URI.join(uri, l).to_s
            rescue
            end
          end
        end
      end
      ret[:links] = links
      ret[:is_success] = true     
      ret
    rescue => e
      @logger.error("scrape failed")
      nil
    end
  end
end
