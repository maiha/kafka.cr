require "./*"

module Kafka::Protocol::Request
  abstract class Base
    include Kafka::Protocol::Format
    include Kafka::Protocol::Request::Format
    include Kafka::Protocol::Request::Macros

    # +--------+--------+----------------+-------------------+
    # | key    | api    | correlation    |(head)  | client   |
    # +--------+--------+----------------+-------------------+
    #  unt16    unt16    unt32            unt16    bytes

    macro request(no)
      field api_key        : Int16 , {{no}}  # request no
      field api_version    : Int16 , 0          # current api version is 0
      field correlation_id : Int32 , 1
      field client_id      : String, "cr"

      {{ yield }}
    end

    macro def to_bytes(io : IO) : Nil
      {% for m in @type.methods.map(&.name).select { |s| s =~ /^write_field_/ } %}
        {{ m }}(io)
      {% end %}
      nil
    end
  end
end
