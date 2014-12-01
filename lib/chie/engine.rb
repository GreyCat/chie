require 'mysql2'
require 'json'

require 'chie/entity'

module Chie
  ##
  # Internal Chie database error. Usually a situation that should
  # never happen and designates either a major data corruption or a
  # (more likely) bug in Chie.
  class InternalError < Exception; end

  ##
  # Top-level object that handles the connection to the database,
  # management of data structure scheme and accessing individual
  # entities for read/write operations.
  class Engine
    DESC_TABLE = '_desc'
    DESC_COLUMN = 'json'

    ##
    # Initializes the new engine and starts the connection to the
    # database.
    #
    # @overload initialize(opts)
    #   Connects to the database, specified using hash.
    #   @param [Hash] opts the options to specify database connection.
    #   @option opts [String] :host SQL server hostname
    #   @option opts [Fixnum] :port SQL server port number
    #   @option opts [String] :username SQL server login to authorize as
    #   @option opts [String] :password SQL server password to authorize as
    #   @option opts [String] :database SQL server database name to use
    #
    # @overload initialize(cred)
    #   Connects to the database, specified using URL string. String
    #   should match general "DATABASE_URL" pattern, i.e. something like
    #   `http://user:pass@host:port/database`
    #   @param [String] cred database URL string
    def initialize(cred)
      if cred.is_a?(String)
        uri = URI.parse(cred)
        raise "Chie is (yet) unable to work with '#{uri.scheme}', it supports only 'mysql2' so far" unless uri.scheme == 'mysql2'
        cred = {
          :host => uri.host,
          :port => uri.port,
          :username => uri.user,
          :password => uri.password,
          :database => uri.path.gsub(/^\//, ''),
          :reconnect => true,
        }
      end

      @db = Mysql2::Client.new(cred)
      desc_parse(desc_read)
    end

    # ========================================================================

    def desc_read
      if sql_table_exists(DESC_TABLE)
        desc_txt = nil
        @db.query("SELECT `#{DESC_COLUMN}` FROM `#{DESC_TABLE}`;").each { |row|
          desc_txt = row[DESC_COLUMN]
        }
        if desc_txt
          JSON.load(desc_txt)
        else
          desc_new
        end
      else
        @db.query("CREATE TABLE `#{DESC_TABLE}` (`#{DESC_COLUMN}` MEDIUMTEXT) DEFAULT CHARSET=utf8;")
        desc_new
      end
    end

    def desc_parse(desc)
      @entities = {}
      desc['entities'].each_pair { |k, v|
        ent = Entity.new(k, v)
        ent.db = @db
        @entities[k] = ent
      }
    end

    def desc_new
      {
        'version' => 1,
        'entities' => {},
      }
    end

    def desc_save
      desc = {
        'version' => 1,
        'entities' => @entities,
      }
      desc_txt = @db.escape(desc.to_json)
      @db.query("TRUNCATE TABLE `#{DESC_TABLE}`;")
      @db.query("INSERT INTO `#{DESC_TABLE}` VALUES ('#{desc_txt}');")
    end

    # ========================================================================

    ##
    # Gets entity by name in this engine.
    # @param [String] name name of the entity
    # @return [Entity]
    def entity(name)
      @entities[name]
    end

    ##
    # Executes some block for each available entity in this engine.
    # @yield [v] Gives entity to the block
    def each_entity(&block)
      @entities.each_pair { |k, v|
        yield(v)
      }
    end

    def entity_create(ent)
      ent.db = @db

      raise "Duplicate entity #{ent.name.inspect}" if @entities[ent.name]

      @db.query("CREATE TABLE `#{ent.name}` (#{ent.schema2sql}) DEFAULT CHARSET=utf8;")
      @db.query <<-__EOS__
      CREATE TABLE `#{ent.name}_h` (
        hid INT NOT NULL AUTO_INCREMENT,
        _id INT NOT NULL,
        PRIMARY KEY (hid),
        INDEX _id_idx (_id),
        _data MEDIUMTEXT,
        ts INT,
        user_id INT
      ) DEFAULT CHARSET=utf8;
      __EOS__

      # Create tables for multi relations
      ent.each_rel { |r|
        next unless r.multi?
        @db.query <<-__EOS__
        CREATE TABLE `#{r.name}` (
          `#{ent.name}` INT NOT NULL,
          `#{r.target}` INT NOT NULL,
          INDEX idx_1 (`#{ent.name}`),
          INDEX idx_2 (`#{r.target}`)
        ) DEFAULT CHARSET=utf8;
        __EOS__
      }

      @entities[ent.name] = ent
      desc_save

      ent
    end

    def entity_delete(name)
      ent = @entities[name]
      raise NotFound.new("Entity #{name} not found") unless ent

      # TODO: add dependency checks here

      @db.query("DROP TABLE `#{name}`;")
      @db.query("DROP TABLE `#{name}_h`;")

      @entities.delete(name)
      desc_save
    end

    # ========================================================================

    def self.validate_sql_name(s)
      raise "Invalid SQL name: #{s.inspect}" unless s =~ /^[A-Za-z_][A-Za-z_0-9]*$/
    end

    def sql_table_exists(name)
      r = false
      @db.query("SHOW TABLES LIKE '#{@db.escape(name)}';").each { |row|
        r = true
      }
      return r
    end
  end
end
