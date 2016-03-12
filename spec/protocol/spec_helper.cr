require "../spec_helper"

def partition_message(partition : Int32, bytes : Array(Slice(UInt8)))
  ms = bytes.map{|b| Structure::MessageSet.new(offset = 0.to_i64, Message.new(b))}
  Structure::PartitionMessage.new(partition, Structure::MessageSetEntry.new(ms))
end

def partition_message(partition : Int32, bytes : Slice(UInt8))
  partition_message(partition, [bytes])
end

def partition_message
  [] of Structure::PartitionMessage
end
