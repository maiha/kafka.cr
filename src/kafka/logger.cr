class Kafka
  @@logger : Logger = Logger.new(STDOUT)

  delegate logger, to: Kafka
  
  def self.logger
    @@logger
  end

  def self.logger=(v)
    @@logger = v
  end

  def logger=(v)
    @@logger = v
  end

  @@logger_debug_prefix : String = ""

  def self.logger_debug_prefix
    @@logger_debug_prefix
  end

  def self.logger_debug_prefix=(v)
    @@logger_debug_prefix = v
  end

  @@logger_debug_level : Int32 = 0

  def self.logger_debug_level
    @@logger_debug_level
  end

  def self.logger_debug_level=(v)
    @@logger_debug_level = v
  end

  @@logger_debug_level_default : Int32 = -1
  def self.logger_debug_level_default
    @@logger_debug_level_default
  end

  def self.logger_debug_level_default=(v)
    @@logger_debug_level_default = v
  end

  @@logger_hexdump : Bool = false
  def self.logger_hexdump
    @@logger_hexdump
  end

  def self.logger_hexdump=(v)
    @@logger_hexdump = v
  end
end
