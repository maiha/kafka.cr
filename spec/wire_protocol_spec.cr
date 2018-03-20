require "./spec_helper"

# https://kafka.apache.org/protocol
describe "wire protocol" do
  include Kafka::Protocol

  it "should work" do
    puts "testing wire_protocol..."

    Dir["#{__DIR__}/../data/**/*.pcap"].each do |pcap|
      # ".../kafka.cr/spec/packet/data/produce/5/req.pcap"
      array = pcap.split("/")              # [...,"produce","5","res.pcap"]
      file  = array.pop                    # "req.pcap"
      text  = pcap.sub(/pcap$/, "text")    # "req.text"
      type  = File.basename(file, ".pcap") # "req"
      ver   = array.pop.to_i               # 5
      key   = array.pop                    # "produce"
      title = "#{key} #{type} (Version: #{ver})"

      got = String.build do |logger|
        Kafka.logger = logger
        Kafka.logger_hexdump = true
        Kafka.debug = true
        io = Kafka::Protocol.from_kafka(File.open(pcap))
        klass = Kafka::Protocol.request(Kafka::Api.parse(key).value, ver)
        case type
        when "req" ; klass.from_kafka(io)
        when "res" ; klass.response.from_kafka(io)
        else       ; "BUG: invalid fixture data. unknown type: #{type}"
        end            
      end
      exp = File.read(text)

      begin
        if got == exp
          puts "  #{title} [OK]".colorize(:green)
        else
          puts "  #{title} [NG]".colorize(:red)
        end
      rescue err
        puts "  #{title} [BUG] #{err}".colorize(:red)
      end
    end
  end
end
