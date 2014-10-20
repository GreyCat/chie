require 'rubygems'
require 'bundler/setup'

require 'rexml/document'

require 'ruby_document_database'
include RubyDocumentDatabase

RSpec.configure { |config|
  # some (optional) config here
}

CREDENTIALS = {
  :host => 'localhost',
  :username => 'rdd_test',
  :password => 'rdd_test',
  :database => 'rdd_test',
}

def sqlexec(cred, cmd)
  `mysql --user='#{cred[:username]}' --password='#{cred[:password]}' '#{cred[:database]}' -e '#{cmd}'`
end

def sqldump(cred)
  dump_xml = `mysqldump --xml --user='#{cred[:username]}' --password='#{cred[:password]}' '#{cred[:database]}'`
  doc = REXML::Document.new(dump_xml)
end
