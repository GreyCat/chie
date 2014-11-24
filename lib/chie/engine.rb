require 'mysql2'
require 'json'

require 'chie/entity'

module Chie
  class InternalError < Exception; end

  class Engine
    DESC_TABLE = '_desc'
    DESC_COLUMN = 'json'

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

    def entity(name)
      @entities[name]
    end

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
