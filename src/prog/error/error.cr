require "../app"

class Error < App
  include Options

  option list : Bool, "-l", "--list", "Show all errors", false
  options :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [codes]

Options:

Example:
  #{PROGRAM_NAME} 6
  #{PROGRAM_NAME} -l
EOF

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
        logger.error err.to_s
      end
    end
  end

  def execute
    if list
      do_list
    else
      do_show(args)
    end
  end
end
