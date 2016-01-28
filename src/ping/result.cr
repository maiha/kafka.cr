module Ping
  class Result
    enum Code
      OK
      KO
      ER
    end

    getter :code, :state

    def initialize(@code : Code, @state : String)
    end
  end
end
