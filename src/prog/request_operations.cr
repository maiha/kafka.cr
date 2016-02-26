module RequestOperations
  protected def execute(request)
    bytes = request.to_slice
    connect do |socket|
      spawn do
        socket.write bytes
        socket.flush
        sleep 0
      end
      return request.class.response.from_kafka(socket)
    end
  end

  protected def resolve_leader!(topic, partition)
    meta = fetch_topic_metadata([topic], app_name)
    meta.topics.each do |t|
      if t.name == topic
        t.partitions.each do |p|
          if p.id == partition
            if p.error_code == -1
              errmsg = Kafka::Protocol.errmsg(p.error_code)
              raise "#{t.name}##{p.id}\t#{errmsg}"
            else
              return meta.broker!(p.leader)
            end
          end
        end
      end
    end

    raise "[BUG?] leader not found for #{topic}##{partition}"
  end
end
