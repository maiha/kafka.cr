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

macro io_read_uint32
  io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
end

macro io_read_int64
  io.read_bytes(Int64, IO::ByteFormat::BigEndian)
end

macro debug_level_succ
  ((debug_level == -1) ? -1 : debug_level + 1)
end

macro on_debug_head_padding
  if debug_level >= 0
    Kafka.logger_debug_prefix = " " * 9
  else
    Kafka.logger_debug_prefix = ""
  end
end

macro debug_addr(v)
  if Kafka.logger_hexdump
    offset = 4          # kafka packet margin for request | response
    "%x" % ({{v}} + offset)
  else
    {{v}}
  end
end

macro on_debug_head_address(base = nil, abs = nil)
  if debug_level >= 0
    {% if abs %}
      _addr_ = {{abs}}
    {% elsif base %}
      _addr_ = {{base}} + io.pos
    {% else %}
      _addr_ = io.pos
    {% end %}
    if Kafka.logger_hexdump
      offset = 4          # kafka packet margin for request | response
      Kafka.logger_debug_prefix = "(%07x)" % (_addr_ + offset)
    else
      Kafka.logger_debug_prefix = "(%07d)" % (_addr_)
    end
  else
    Kafka.logger_debug_prefix = ""
  end
end

macro on_debug_label
  _label = self.to_s.sub(/^.*::/, "")
  _label = _label.sub(/(Request|Response)(V(\d+))?$/){
    "#{$1} (Version: #{$~[3]? || 0})"
  }
  on_debug _label
end

macro on_debug(msg)
  if debug_level >= 0
    buf = String.build do |_io_|
      _io_.print Kafka.logger_debug_prefix
      _io_.print " " * (debug_level * 2)
      _io_.print {{msg.id}}.to_s.colorize(:yellow)
    end
    Kafka.logger.debug buf
  end
end

macro io_read_bytes_with_debug(color, type)
  name = hint.to_s.empty? ? "" : "(#{hint})"
  bytes = {
    "Int64"  => 8,
    "Int32"  => 4,
    "UInt32" => 4,
    "Int16"  => 2,
    "Int8"   => 1,
    "Bool"   => 1,
  }[{{type.id}}.to_s] || "?"
  label = {{type.id}}.to_s + "[#{bytes}]#{name}"
  begin
    {% if type.id.stringify == "Bool" %}
      value = io.read_bytes(Int8, IO::ByteFormat::BigEndian)
    {% else %}
      value = io.read_bytes({{type.id}}, IO::ByteFormat::BigEndian) # {{type.id}}
    {% end %}
    if hint.to_s == "error_code" && value != 0
      errmsg = Kafka::Protocol.errmsg(value.to_i16)
      on_debug "#{label} -> #{errmsg}".colorize(:red)
    else
      on_debug "#{label} -> #{value}".colorize({{color}})
    end

    {% if type.stringify == "Bool" %}
      return (value == 1)
    {% else %}
      return value
    {% end %}
  rescue err
    on_debug "#{label} (#{err})".colorize(:red)
    raise err
  end
end

macro io_read_bytes_with_debug(color)
  io_read_bytes_with_debug({{color}}, self)
end
