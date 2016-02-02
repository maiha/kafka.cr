require "option_parser"
require "socket"
require "../kafka"
require "./macros"
require "./options"

abstract class App
  abstract def execute

  def self.run(args)
    new(args).run
  end

  getter :args

  def initialize(@args)
  end

  def run
    parse_args!
    execute
  rescue err
    die err
  end
end
