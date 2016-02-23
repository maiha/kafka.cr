module Kafka::Protocol::Structure
  ######################################################################
  ### Codec

  def MessageSetEntry.from_kafka(io : IO, debug_level = -1, hint = "")
    on_debug_head_padding
    on_debug "MessageSetEntry".colorize(:cyan)
    size = Int32.from_kafka(io, debug_level_succ, :size)
    sets = [] of MessageSet

    start_pos = io.pos
    limit_pos = start_pos + size
    begin
      loop do
        break if io.pos == io.bytesize  # maybe no more MessageSet
        sets << MessageSet.from_kafka(io, debug_level_succ, :message_set)
      end
    rescue err : IO::EOFError
      if io.pos == limit_pos
        on_debug_head_address
        on_debug "[DONE] expected offset=#{limit_pos}(size=#{size}), current offset=#{io.pos}"
        # expected size
      else
        raise err
      end
    end
    return new(size, sets)
  end
end
