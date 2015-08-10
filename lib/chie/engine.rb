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
    DESC_VER_COLUMN = 'ver'

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
      @desc_ver = nil
      desc_parse(desc_read)
    end

    ##
    # Workaround method to access SQL connection directly.
    def query_sql(q)
      @db.query(q)
    end

    ##
    # Refreshes in-memory Chie schema cache in this engine
    # instance. Interactive multi-user applications (i.e. web
    # applications) that allow schema changes by users should call
    # this frequently (ideally, once per incoming request, before
    # doing anything with the database in this request) to make sure
    # that all application instances use most up-to-date schema.  This
    # method is optimized - i.e. checks whether JSON schema has
    # changed and won't re-read JSON schema if it didn't.
    def refresh!
      desc_parse(desc_read) if desc_need_update
    end

    ##
    # Performs a given block inside a transaction enforced by
    # underlying RDBMS. Successfully exiting a block will result in
    # COMMIT being executed, thus committing everything that happened
    # in a block to the database. Any uncaught exception that will
    # happen will force a ROLLBACK being executed, thus rolling back
    # all the changes that happened to the database as it were when
    # the block was just starting.
    def transaction(&block)
      @db.query('START TRANSACTION;')
      begin
        yield
        @db.query('COMMIT;')
      rescue Exception => e
        @db.query('ROLLBACK;')
        raise e
      end
    end

    # ========================================================================

    def desc_need_update
      new_ver = 0
      @db.query("SELECT `#{DESC_VER_COLUMN}` FROM `#{DESC_TABLE}`;").each { |row|
        new_ver = row[DESC_VER_COLUMN]
      }

      @desc_ver != new_ver
    end

    def desc_read
      if sql_table_exists(DESC_TABLE)
        desc_txt = nil
        @db.query("SELECT `#{DESC_COLUMN}`, `#{DESC_VER_COLUMN}` FROM `#{DESC_TABLE}`;").each { |row|
          desc_txt = row[DESC_COLUMN]
          @desc_ver = row[DESC_VER_COLUMN]
        }
        if desc_txt
          JSON.load(desc_txt)
        else
          @desc_ver = 0
          desc_new
        end
      else
        @db.query("CREATE TABLE `#{DESC_TABLE}` (`#{DESC_COLUMN}` MEDIUMTEXT, `#{DESC_VER_COLUMN}` DOUBLE) DEFAULT CHARSET=utf8;")
        desc_new
      end
    end

    def desc_parse(desc)
      @entities = {}
      desc['entities'].each_pair { |k, v|
        ent = Entity.new(k, v)
        ent.db = @db
        ent.engine = self
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
      @db.query("INSERT INTO `#{DESC_TABLE}` VALUES ('#{desc_txt}', #{Time.now.to_f});")
    end

    # ========================================================================

    ##
    # Gets entity by name in this engine.
    # @param [String] name name of the entity
    # @return [Entity, nil] entity if it exists, nil otherwise
    def entity(name)
      @entities[name]
    end

    ##
    # Gets entity by name in this engine. Fails with an exception if
    # requested entity does not exist.
    # @param [String] name name of the entity
    # @return [Entity] entity
    def entity!(name)
      entity(name) or raise NotFound.new("Entity #{name.inspect} not found")
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
      ent.engine = self

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
        CREATE TABLE `#{r.sql_table}` (
          `#{r.sql_column1}` INT NOT NULL,
          `#{r.sql_column2}` INT NOT NULL,
          INDEX idx_1 (`#{r.sql_column1}`),
          INDEX idx_2 (`#{r.sql_column2}`),
          PRIMARY KEY (`#{r.sql_column1}`, `#{r.sql_column2}`)
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

      # Remove extra tables for multi-relations
      ent.each_rel { |r|
        @db.query("DROP TABLE `#{r.sql_table}`;") if r.multi?
      }

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

    ##
    # Converts a given value to a string, safe to be used in SQL statement.
    # @return [String, nil] escaped and quoted string, safe to be used
    #   in SQL statement, or nil if the conversion wasn't possible
    def escape_value(v)
      if v.is_a?(String)
        "'#{@db.escape(v)}'"
      elsif v.is_a?(Numeric)
        v.to_s
      else
        nil
      end
    end

    ##
    # Escapes literate name as per standards of current SQL engine.
    # @param [String] v table or column name to escape
    # @return [String] escaped name
    def escape_name(v)
      "`#{v}`"
    end
  end
end
