require 'mysql2'
require 'json'

require 'ruby_document_database/entity'

module RubyDocumentDatabase
  class InternalError < Exception; end

  class Engine
    DESC_TABLE = '_desc'
    DESC_COLUMN = 'json'

    def initialize(cred)
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
        @db.query("CREATE TABLE `#{DESC_TABLE}` (`#{DESC_COLUMN}` MEDIUMTEXT);")
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

      raise "Duplicate entity #{name.inspect}" if @entities[ent.name]

      @db.query("CREATE TABLE `#{ent.name}` (#{ent.schema2sql});")
      @db.query <<-__EOS__
      CREATE TABLE `#{ent.name}_h` (
        hid INT NOT NULL AUTO_INCREMENT,
        _id INT NOT NULL,
        PRIMARY KEY (hid),
        INDEX _id_idx (_id),
        _data MEDIUMTEXT,
        ts DATETIME,
        user_id INT
      );
      __EOS__

      @entities[ent.name] = ent
      desc_save

      ent
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
