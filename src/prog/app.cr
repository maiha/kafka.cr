require "./deps"

abstract class App
  abstract def execute

  include RequestOperations
  include ResponseOperations
  
  def self.run(args)
    new(args).run
  end

  getter :args

  def initialize(@args)
  end

  def run
    parse_args!
    execute
  rescue err : Options::OptionError
    die err
  rescue err
    die err, show_usage: false
  ensure
    STDOUT.flush
    STDERR.flush
  end

    protected def app_name
    "kafka-#{self.class.name.downcase}"
  end

  protected def show_version
    STDERR.puts "#{$0} #{Kafka::Info::VERSION}"
    STDERR.puts "License #{Kafka::Info::LICENSES}"
    STDERR.puts "Written by #{Kafka::Info::AUTHORS} (#{Kafka::Info::HOMEPAGE})"
    exit 0
  end
  
  protected def die(msg, show_usage = true)
    STDERR.puts "ERROR: #{msg}\n".colorize(:red) unless msg.to_s.empty?
    STDERR.puts usage if show_usage
    STDERR.flush
    exit
  end
end
