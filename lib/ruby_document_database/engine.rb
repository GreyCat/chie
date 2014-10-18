require 'mysql2'

module RubyDocumentDatabase
  class Engine
    def initialize(db)
      @db = db
    end

    def self.connect_mysql(cred)
      db = Mysql2::Client.new(cred)
      self.new(db)
    end
  end
end
