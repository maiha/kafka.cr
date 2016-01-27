module Ping
  record Dest, host, port do
    def self.parse(str : String) : Dest
      if str.includes?(":")
        host, port = str.split(":", 2)
        new(host, port.to_i)
      else
        Dest.new(str, 9092)
      end
    end
  end
end
