require 'mysql2'
require 'json'

require 'ruby_document_database/entity'

module RubyDocumentDatabase
  class Engine
    DESC_TABLE = '_desc'
    DESC_COLUMN = 'json'

    attr_reader :entities

    def initialize(cred)
      @db = Mysql2::Client.new(cred)
      desc_read
      desc_parse
    end

    # ========================================================================

    def desc_read
      if sql_table_exists(DESC_TABLE)
        desc_txt = nil
        @db.query("SELECT `#{DESC_COLUMN}` FROM `#{DESC_TABLE}`;").each { |row|
          desc_txt = row[DESC_COLUMN]
        }
        if desc_txt
          @desc = JSON.load(desc_txt)
        else
          @desc = desc_new
          desc_save
        end
      else
        @db.query("CREATE TABLE `#{DESC_TABLE}` (`#{DESC_COLUMN}` MEDIUMTEXT);")
        @desc = desc_new
        desc_save
      end
    end

    def desc_parse
      @entities = {}
      @desc['entities'].each_pair { |k, v|
        @entities[k] = Entity.new(@db, k, v)
      }
    end

    def desc_new
      {
        'version' => 1,
        'entities' => {},
      }
    end

    def desc_save
      desc_txt = @db.escape(@desc.to_json)
      @db.query("TRUNCATE TABLE `#{DESC_TABLE}`;")
      @db.query("INSERT INTO `#{DESC_TABLE}` VALUES ('#{desc_txt}');")
    end

    # ========================================================================

    def entity_create(name, schema)
      validate_sql_name(name)
      ent = Entity.new(@db, name, schema)

      raise "Duplicate entity #{name.inspect}" if @entities[name]

      @db.query("CREATE TABLE `#{name}` (#{schema2sql(schema)});")
      @db.query <<-__EOS__
      CREATE TABLE `#{name}_h` (
        hid INT NOT NULL AUTO_INCREMENT,
        _id INT NOT NULL,
        PRIMARY KEY (hid),
        INDEX _id_idx (_id),
        _data MEDIUMTEXT,
        ts DATETIME,
        user_id INT
      );
      __EOS__

      @entities[name] = ent
      desc_save

      ent
    end

    # ========================================================================

    def schema2sql(schema)
      lines = [
        '_id INT NOT NULL AUTO_INCREMENT',
        'PRIMARY KEY (_id)',
        '_data MEDIUMTEXT',
      ]
      schema.each { |v|
        validate_sql_name(v[:name])
        sql_type = case v[:type]
        when :str
          len = v[:len] || 256
          "VARCHAR(#{len})"
        when :int
          'INT'
        else
          raise "Invalid type #{v[:type].inspect} encountered on attribute #{v[:name].inspect}"
        end
        lines << "#{v[:name]} #{sql_type}"
      }
      lines.join(', ')
    end

    def validate_sql_name(s)
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
