class CompositeLogger < Logger
  include Enumerable(Logger)

  def initialize(@loggers : Array(Logger) = loggers)
    super(nil)
  end

  delegate each, to: @loggers

  {% for method in %w( level= formatter= ) %}
    def {{method.id}}(*args)
      each do |logger|
        logger.{{method.id}}(*args)
      end
    end
  {% end %}

  {% for method in %w( debug info warn error fatal ) %}
    def {{method.id}}(*args, **options)
      each do |logger|
        logger.{{method.id}}(*args, **options)
      end
    end

    def {{method.id}}(*args, **options)
      each do |logger|
        logger.{{method.id}}(*args, **options) do |*yield_args|
          yield *yield_args
        end
      end
    end
  {% end %}
end

def CompositeLogger.stdio
  stdout = Logger.new(STDOUT).tap(&.level = Logger::DEBUG)
  stderr = Logger.new(STDERR).tap(&.level = Logger::WARN)
  logger = CompositeLogger.new([stdout, stderr])
  logger.formatter = Logger::Formatter.new do |level, time, progname, message, io|
    io << message
  end
  return logger
end
