class Kafka
  # TODO: move to each logger instances
  def self.debug=(v)
    if v
      logger = Logger.new(STDOUT).tap(&.level = Logger::DEBUG)
      logger.formatter = Logger::Formatter.new do |level, time, progname, message, io|
        io << message
      end
      Kafka.logger = logger
      Kafka.logger_debug_level_default = 0
    else
      Kafka.logger_debug_level_default = -1
    end
  end
end
