require "option_parser"
require "socket"
require "../kafka"

class Error::Main
  include Kafka::Protocol

  getter :args, :usage, :help, :list

  def initialize(@args)
    @list = false
    @usage = false
    
    opts = OptionParser.parse(args) do |parser|
      parser.on("-l", "--list", "Show all errors") { @list = true }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @help = -> {
      STDERR.puts "Usage: kafka-error [options] [codes]"
      STDERR.puts ""
      STDERR.puts "Options:"
      STDERR.puts opts
      STDERR.puts "\n"
      STDERR.puts <<-EXAMPLE
        Example:
          #{$0} 6
          #{$0} -l
        EXAMPLE
      exit
    }
  end

  def do_list
    Kafka::Protocol::Errors.values.each do |value|
      puts "#{value.to_i}\t#{value}"
    end
  end

  def do_show(codes)
    codes.each do |code|
      begin
        value = Kafka::Protocol::Errors.from_value(code.to_i)
        puts "#{value.to_i}\t#{value}"
      rescue err
        STDERR.puts err.to_s
      end
    end
  end

  def run
    if usage
      help.call
    end

    if list
      do_list
      return
    end

    if args.any?
      do_show(args)
      return
    end

    die "no error codes given"
    
  rescue err : Errno
    die err
  end

  private def die(msg)
    STDERR.puts "ERROR: #{msg}\n\n"
    help.call
  end
end

module Error
  def self.run(args)
    Main.new(args).run
  ensure
    STDOUT.flush
    STDERR.flush
  end
end
