module ResponseOperations
  protected def not_leader?(res)
    res.topics.each do |t|
      t.partitions.each do |p|
        return true if p.error_code == 6
      end
    end
    return false
  end

  protected def print_res(res : Kafka::Protocol::FetchResponse, guess = false : Bool)
    res.topics.each do |t|
      t.partitions.each do |p|
        head = "#{t.topic}##{p.partition}"
        if p.error_code == 0
          p.message_sets.each do |m|
            bytes = m.message.value
            value = guess ? guess_binary(bytes) : bytes
            STDOUT.puts "#{head}\t#{m.offset}: #{value.to_s}"
          end
        else
          errmsg = Kafka::Protocol.errmsg(p.error_code)
          STDOUT.puts "#{head}\t#{errmsg}".colorize(:red)
        end
      end
    end
  end
end
