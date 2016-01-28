require "./spec_helper"
require "../../src/ping"

describe Ping do
  describe "SingleHost" do
    it "can be compiled" do
      Ping::SingleHost
    end
  end

  describe "MultiHosts" do
    it "can be compiled" do
      Ping::MultiHosts
    end
  end
end
