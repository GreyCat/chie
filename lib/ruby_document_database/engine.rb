require 'mysql2'
require 'json'

module RubyDocumentDatabase
  class Engine
    DESC_TABLE = '_desc'
    DESC_COLUMN = 'json'

    attr_reader :desc

    def initialize(db)
      @db = db
      desc_read
    end

    def self.connect_mysql(cred)
      db = Mysql2::Client.new(cred)
      self.new(db)
    end

    # ========================================================================

    def desc_read
      if sql_table_exists(DESC_TABLE)
        desc_txt = nil
        @db.query('SELECT `#{DESC_COLUMN}` FROM `#{DESC_TABLE}`;') { |row|
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

      raise "Duplicate entity #{name.inspect}" if @desc['entities'][name]

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

      @desc['entities'][name] = schema
      desc_save
    end

    def entities
      @desc['entities'].keys
    end

    def entity_get(name)
      @desc['entities'][name]
    end

    def entity_count(name)
      validate_sql_name(name)

      cnt = nil
      @db.query("SELECT COUNT(*) AS cnt FROM `#{name}`;").each { |row|
        cnt = row['cnt']
      }
      raise "Invalid query result returned from counting rows in #{name.inspect}" if cnt.nil?

      cnt
    end

    # ========================================================================

    def insert(entity, data, user = nil)
      schema = @desc['entities'][entity]
      raise "Unknown entity #{entity.inspect}" if schema.nil?

      cols = parse_data_with_schema(data, schema)
      col_names = cols.keys.map { |x| "`#{x}`" }.join(',')
      col_vals = cols.values.join(',')

      if user.nil?
        user = 'NULL'
      elsif not user.is_a?(Fixnum)
        raise "Unable to use user ID #{user.inspect}"
      end

      @db.query("INSERT INTO `#{entity}` (#{col_names}) VALUES (#{col_vals});")
      id = @db.last_id
      @db.query("INSERT INTO `#{entity}_h` (_id, _data, ts, user_id) VALUES (#{id}, #{cols['_data']}, NOW(), #{user});")

      id
    end

    def update(entity, id, data, user = nil)
      schema = @desc['entities'][entity]
      raise "Unknown entity #{entity.inspect}" if schema.nil?

      raise "Unable to use ID #{id.inspect}" unless id.is_a?(Fixnum)

      cols = parse_data_with_schema(data, schema)
      cols['_id'] = id
      col_names = cols.keys.map { |x| "`#{x}`" }.join(',')
      col_vals = cols.values.join(',')

      if user.nil?
        user = 'NULL'
      elsif not user.is_a?(Fixednum)
        raise "Unable to use user ID #{user.inspect}"
      end

      @db.query("DELETE FROM `#{entity}` WHERE _id=#{id};")
      @db.query("INSERT INTO `#{entity}` (#{col_names}) VALUES (#{col_vals});")
      @db.query("INSERT INTO `#{entity}_h` (_id, _data, ts, user_id) VALUES (#{id}, #{cols['_data']}, NOW(), #{user});")
    end

    def get(entity, id)
      raise "ID must be integer, but got #{id.inspect}" unless id.is_a?(Fixnum)
      r = nil
      @db.query("SELECT _data FROM `#{entity}` WHERE _id=#{id};").each { |row|
        r = row['_data']
      }
      raise "Invalid query result returned from getting data on ID=#{id}" if r.nil?

      JSON.load(r)
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

    def parse_data_with_schema(data, schema)
      r = {
        '_data' => "'#{@db.escape(data.to_json)}'",
      }

      attr_by_name = {}
      schema.each { |attr|
        k = attr[:name]
        raise "Weird schema: no name in attribute #{attr.inspect}" unless k
        raise "Weird schema: duplicate attribute #{attr.inspect}" if attr_by_name[k]
        attr_by_name[k] = attr
      }

      data.each_pair { |k, v|
        attr = attr_by_name[k]
        raise "Unknown attribute #{k.inspect}" if attr.nil?

        sql_value = case attr[:type]
        when :str
          "'#{@db.escape(v)}'"
        when :int
          v
        else
          raise "Invalid type #{attr[:type].inspect} encountered on attribute #{k.inspect}"
        end

        r[k] = sql_value
      }

      return r
    end

    def validate_sql_name(s)
      raise "Invalid SQL name: #{s.inspect}" unless s =~ /^[A-Za-z_][A-Za-z_0-9]*$/
    end

    def sql_table_exists(name)
      r = false
      @db.query("SHOW TABLES LIKE '#{@db.escape(name)}';") { |row|
        r = true
      }
      return r
    end
  end
end
