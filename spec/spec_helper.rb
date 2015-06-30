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
  :username => 'chie_test',
  :password => 'chie_test',
  :database => 'chie_test',
}

DATABASE_URL = "mysql2://#{CREDENTIALS[:user]}:#{CREDENTIALS[:password]}@#{CREDENTIALS[:host]}/#{CREDENTIALS[:database]}"

def sqlexec(cmd)
  `mysql --user='#{CREDENTIALS[:username]}' --password='#{CREDENTIALS[:password]}' '#{CREDENTIALS[:database]}' -e '#{cmd}'`
end

def sqlclear
  sqlexec("DROP DATABASE #{CREDENTIALS[:database]}; CREATE DATABASE #{CREDENTIALS[:database]};")
end

def sqldump
  dump_xml = `mysqldump --xml --user='#{CREDENTIALS[:username]}' --password='#{CREDENTIALS[:password]}' '#{CREDENTIALS[:database]}'`
  doc = REXML::Document.new(dump_xml, ignore_whitespace_nodes: :all)
end
