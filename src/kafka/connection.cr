require "socket"

class Kafka::Connection
  def initialize(@host : String, @port : Int32)
    @socket = uninitialized TCPSocket
    @connected = false
  end

  def connected?
    @connected
  end

  def open
    close
    connect
  end

  def close
    if connected?
      @socket.close
      @connected = false
    end
  end

  def write(bytes : Slice(UInt8))
    socket!.write(bytes)
    socket!.flush
  rescue err
    close
    raise err
  end

  def read : Slice
    size = socket!.read_bytes(Int32, IO::ByteFormat::BigEndian)
    body = Slice(UInt8).new(size)
    socket!.read_fully(body)

    out = MemoryIO.new(4 + size)
    out.write_bytes(size, format = IO::ByteFormat::BigEndian)
    out.write(body)
    out.to_slice
  rescue err
    close
    raise err
  end

  def socket!
    open unless connected?
    @socket.not_nil!
  end

  private def connect
    @socket = TCPSocket.new(@host, @port)
    @connected = true
  end
end
