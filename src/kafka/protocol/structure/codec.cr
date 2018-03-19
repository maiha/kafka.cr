module Kafka::Protocol::Structure
  # #####################################################################
  # ## Codec

  def MessageSetEntry.from_kafka(io : IO, debug_level = -1, hint = "")
    debug_head_padding
    debug "MessageSetEntry".colorize(:cyan)
    size = Int32.from_kafka(io, debug_level_succ, :size)
    sets = [] of MessageSet

    start_pos = io.pos
    limit_pos = start_pos + size
    begin
      loop do
        break if io.pos == io.bytesize # maybe no more MessageSet
        sets << MessageSet.from_kafka(io, debug_level_succ, :message_set)
      end
    rescue err : IO::EOFError
      if io.pos == limit_pos
        debu_set_head_address
        debug "[DONE] expected offset=#{limit_pos}(size=#{size}), current offset=#{io.pos}"
        # expected size
      else
        raise err
      end
    end
    return new(size, sets)
  end

  def Varbytes.from_kafka(io : IO, debug_level = -1, hint = "", abs_pos = 0)
    debu_set_head_address(abs: abs_pos)
    name = hint.to_s.empty? ? "" : "(#{hint})"

    v = Varint.decode(io)
    size = v.value

    if size < 0
      debug "Varbytes[#{v.read_bytes}+0]#{name} -> (null)".colorize(:cyan)
      return new(size, Null)
    else
      bytes = Slice(UInt8).new(size).tap { |s| io.read_fully(s) }
      value_hint = String.new(bytes) rescue bytes
      debug "Varbytes[#{v.read_bytes}+#{size}]#{name} -> (#{size})#{value_hint.inspect}".colorize(:cyan)
      return new(size, bytes)
    end
  end

  struct Varbytes
    def to_kafka(io : IO)
      raise "NotImplementedYet"
    end
  end
  
  def VarArray.from_kafka(io : IO, debug_level = -1, hint = "", abs_pos = 0)
    debu_set_head_address(abs: abs_pos)
    label = self.to_s.gsub(/[A-Za-z]+::/, "") # VarArray(Kafka::Protocol::Structure::Header) -> VarArray(Header)
    var = ZigZag::Varint.decode(io)
    len = var.value
    debug "#{label} -> #{len}".colorize(:cyan)

    ary = new
    (1..len).each do
      ary << T.from_kafka(io, debug_level_succ)
    end
    return ary
  end
end

def ZigZag::Var.from_kafka(io : IO, debug_level = -1, hint = "", abs_pos = 0) : Var(T)
  debu_set_head_address(abs: abs_pos)
  name = hint.to_s.empty? ? "" : "(#{hint})"
  var = decode(io)

  type_hint = {{T.name.stringify}}
  type_hint = "Varint"  if type_hint =~ /32/
  type_hint = "Varlong" if type_hint =~ /64/
  
  debug ("#{type_hint}[#{var.read_bytes}]#{name} -> #{var.value}").colorize(:cyan)
  return var
end

class ZigZag::Var(T)
  def to_kafka(io : IO)
    raise "NotImplementedYet"
  end
end
