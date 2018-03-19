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

  def self.debug?
    Kafka.logger_debug_level_default >= 0
  end

  def self.debug2=(v : Bool)
    self.debug = true if v
    @@debug2 = v
  end

  def self.debug2?
    !!@@debug2
  end
end
