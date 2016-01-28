require "option_parser"
require "socket"
require "./kafka"
require "./ping/*"

module Ping
  def self.run
    Main.new.run
  end
end
