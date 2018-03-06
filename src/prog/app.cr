require "./deps"

macro logger
  Kafka.logger
end

abstract class App
  abstract def execute

  include RequestOperations
  include ResponseOperations
  
  def self.run(args)
    new(args).run
  end

  getter :args

  def initialize(@args : Array(String))
    Kafka.logger = Logger.new(STDERR)
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
    logger.error "#{PROGRAM_NAME} #{Kafka::Info::VERSION}"
    logger.error "License #{Kafka::Info::LICENSES}"
    logger.error "Written by #{Kafka::Info::AUTHORS} (#{Kafka::Info::HOMEPAGE})"
    exit 0
  end
  
  protected def die(msg, show_usage = true)
    logger.error "ERROR: #{msg}\n".colorize(:red) unless msg.to_s.empty?
    logger.error usage if show_usage
    STDERR.flush
    exit
  end
end
