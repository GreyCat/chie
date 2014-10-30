require 'rubygems'
require 'bundler/setup'

require 'rexml/document'

require 'chie'
include Chie

RSpec.configure { |config|
  # some (optional) config here
}

CREDENTIALS = {
  :host => 'localhost',
  :username => 'rdd_test',
  :password => 'rdd_test',
  :database => 'rdd_test',
}

def sqlexec(cmd)
  `mysql --user='#{CREDENTIALS[:username]}' --password='#{CREDENTIALS[:password]}' '#{CREDENTIALS[:database]}' -e '#{cmd}'`
end

def sqlclear
  sqlexec("DROP DATABASE #{CREDENTIALS[:database]}; CREATE DATABASE #{CREDENTIALS[:database]};")
end

def sqldump
  dump_xml = `mysqldump --xml --user='#{CREDENTIALS[:username]}' --password='#{CREDENTIALS[:password]}' '#{CREDENTIALS[:database]}'`
  doc = REXML::Document.new(dump_xml)
end
