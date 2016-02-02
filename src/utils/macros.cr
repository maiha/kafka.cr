require "colorize"

macro io_read_int8
  io.read_bytes(Int8, IO::ByteFormat::BigEndian)
end

macro io_read_int16
  io.read_bytes(Int16, IO::ByteFormat::BigEndian)
end

macro io_read_int32
  io.read_bytes(Int32, IO::ByteFormat::BigEndian)
end

macro io_read_int64
  io.read_bytes(Int64, IO::ByteFormat::BigEndian)
end

macro debug_level_succ
  ((debug_level == -1) ? -1 : debug_level + 1)
end

macro on_debug_head_padding
  if debug_level >= 0
    STDERR.printf " " * 9
  end
end

macro on_debug_head_address
  if debug_level >= 0
    STDERR.printf "(%07d)", io.pos
  end
end

macro on_debug(msg)
  if debug_level >= 0
    STDERR.print " " * debug_level * 2
    STDERR.puts {{msg.id}}.to_s.colorize(:yellow)
    STDERR.flush
  end
end

macro io_read_bytes_with_debug(color, type)
  name = hint.to_s.empty? ? "" : "(#{hint})"
  bytes = {
    "Int64" => 8,
    "Int32" => 4,
    "Int16" => 2,
    "Int8"  => 1,
  }[{{type.id}}.to_s] || "?"
  label = "#{self}[#{bytes}]#{name}"
  begin
    value = io.read_bytes({{type.id}}, IO::ByteFormat::BigEndian)
    if hint.to_s == "error_code" && value != 0
      errmsg = Kafka::Protocol.errmsg(value.to_i16)
      on_debug "#{label} -> #{errmsg}".colorize(:red)
    else
      on_debug "#{label} -> #{value}".colorize({{color}})
    end
    return value
  rescue err
    on_debug "#{label} (#{err})".colorize(:red)
    raise err
  end
end

macro io_read_bytes_with_debug(color)
  io_read_bytes_with_debug({{color}}, self)
end
