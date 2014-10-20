require 'rubygems'
require 'bundler/setup'

require 'rexml/document'

require 'ruby_document_database'
include RubyDocumentDatabase

RSpec.configure { |config|
  # some (optional) config here
}

def sqlexec(cred, cmd)
  `mysql --user='#{cred[:username]}' --password='#{cred[:password]}' '#{cred[:database]}' -e '#{cmd}'`
end

def sqldump(cred)
  dump_xml = `mysqldump --xml --user='#{cred[:username]}' --password='#{cred[:password]}' '#{cred[:database]}'`
  doc = REXML::Document.new(dump_xml)
end
