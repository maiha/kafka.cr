class Kafka::Execution
  def self.execute(connection : Kafka::Connection, request, handler)
    handler.request(request)
    bytes = request.to_slice

    # send
    connection.socket! # to make sure that socket has been opened before spawn
    spawn do
      connection.write bytes
      sleep 0
    end
    handler.send(bytes)

    # recv
    recv = connection.read
    handler.recv(recv.to_slice)

    # convert
    response = request.class.response.from_kafka(recv, handler.verbose)
    handler.respond(response)

    handler.completed(request, response)

    return response
  rescue err
    handler.failed(request, err)
    raise err
  end
end

class Kafka
  def execute(request : Kafka::Request, handler : Kafka::Handler)
    Kafka::Execution.execute(connection, request, handler)
  end

  def execute(request : Kafka::Request)
    execute(request, handler)
  end
end
