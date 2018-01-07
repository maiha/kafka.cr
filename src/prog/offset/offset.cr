require "../app"

class Offset < App
  include Options
    
  options :broker, :partition, :topic, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topics]

Options:

Example:
  #{PROGRAM_NAME} topic1
  #{PROGRAM_NAME} topic1 topic2
  #{PROGRAM_NAME} topic1 --broker=localhost:9092
  #{PROGRAM_NAME} topic1 -v
EOF

  def do_list
    do_show([] of String)
  end

  def do_show(topics)
    res = fetch_offset(topics, partition)
    print_res(res)
  end

  def execute
    topics = ([topic] + args).reject(&.empty?)

    if topics.any?
      return do_show(topics)
    end

    die "no topics"
  end
end
