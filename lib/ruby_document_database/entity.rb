require 'ruby_document_database/attribute'
require 'ruby_document_database/relation'

module RubyDocumentDatabase
  class Entity
    attr_reader :name
    attr_accessor :db

    def initialize(name, h)
      Engine::validate_sql_name(name)

      @db = nil
      @name = name
      parse_attr(h['attr'] || [])
      parse_rel(h['rel'] || [])
    end

    def parse_attr(h_attr)
      @attrs = []
      @attr_by_name = {}

      h_attr.each { |a|
        attr = Attribute.new(a)
        raise "Weird schema: duplicate attribute #{attr.inspect}" if @attr_by_name[attr.name]
        @attr_by_name[attr.name] = attr
        @attrs << attr.name
      }
    end

    def parse_rel(h_rel)
      @rels = []
      @rel_by_name = {}

      h_rel.each { |r|
        rel = Relation.new(r)
        raise "Weird schema: duplicate relation name #{rel.inspect}" if @rel_by_name[rel.name]
        @rel_by_name[rel.name] = rel
        @rels << rel.name
      }
    end

    # ========================================================================

    attr_reader :attrs, :rels

    def attr(name)
      @attr_by_name[name]
    end

    def rel(name)
      @rel_by_name[name]
    end

    def each_attr(&block)
      @attrs.each { |k|
        yield(@attr_by_name[k])
      }
    end

    def each_rel(&block)
      @rels.each { |k|
        yield(@rel_by_name[k])
      }
    end

    # ========================================================================

    def count
      cnt = nil
      @db.query("SELECT COUNT(*) AS cnt FROM `#{@name}`;").each { |row|
        cnt = row['cnt']
      }
      raise "Invalid query result returned from counting rows in #{name.inspect}" if cnt.nil?

      cnt
    end

    def get(id)
      validate_id(id)

      r = nil
      @db.query("SELECT _data FROM `#{@name}` WHERE _id=#{id};").each { |row|
        r = row['_data']
      }
      raise "Invalid query result returned from getting data on ID=#{id}" if r.nil?

      JSON.load(r)
    end

    def history_list(id)
      validate_id(id)
      @db.query("SELECT hid, ts, user_id FROM `#{@name}_h` WHERE _id=#{id};")
    end

    def history_get(hid)
      validate_id(hid)

      r = nil
      @db.query("SELECT _data, ts, user_id FROM `#{@name}_h` WHERE hid=#{hid};").each { |row|
        r = row
      }
      raise "Invalid query result returned from getting historical data on HID=#{hid}" if r.nil?

      h = JSON.load(r['_data'])
      h['_ts'] = r['ts']
      h['_user'] = r['user_id']

      h
    end

    def insert(data, user = nil)
      user = parse_user(user)

      cols = parse_data_with_schema(data)
      col_names = cols.keys.map { |x| "`#{x}`" }.join(',')
      col_vals = cols.values.join(',')

      @db.query("INSERT INTO `#{@name}` (#{col_names}) VALUES (#{col_vals});")
      id = @db.last_id
      @db.query("INSERT INTO `#{@name}_h` (_id, _data, ts, user_id) VALUES (#{id}, #{cols['_data']}, NOW(), #{user});")

      id
    end

    def update(id, data, user = nil)
      validate_id(id)
      user = parse_user(user)

      cols = parse_data_with_schema(data)
      cols['_id'] = id
      col_names = cols.keys.map { |x| "`#{x}`" }.join(',')
      col_vals = cols.values.join(',')

      @db.query("DELETE FROM `#{@name}` WHERE _id=#{id};")
      @db.query("INSERT INTO `#{@name}` (#{col_names}) VALUES (#{col_vals});")
      @db.query("INSERT INTO `#{@name}_h` (_id, _data, ts, user_id) VALUES (#{id}, #{cols['_data']}, NOW(), #{user});")
    end

    # ========================================================================

    def to_json(opt = nil)
      a = []
      each_attr { |v| a << v }
      {
        'attr' => a,
#        'rel' => @rels,
      }.to_json(opt)
    end

    # ========================================================================

    def schema2sql
      lines = [
        '_id INT NOT NULL AUTO_INCREMENT',
        'PRIMARY KEY (_id)',
        '_data MEDIUMTEXT',
      ]
      each_attr { |a|
        lines << "#{a.name} #{a.as_sql_type}"
      }
      lines.join(', ')
    end

    private
    def validate_id(id)
      raise "ID must be integer, but got #{id.inspect}" unless id.is_a?(Fixnum)
    end

    def parse_data_with_schema(data)
      r = {
        '_data' => "'#{@db.escape(data.to_json)}'",
      }

      data.each_pair { |k, v|
        attr = @attr_by_name[k]
        raise "Unknown attribute #{k.inspect}" if attr.nil?

        sql_value = case attr.type
        when 'str'
          "'#{@db.escape(v)}'"
        when 'int'
          v
        else
          raise "Invalid type #{attr.type.inspect} encountered on attribute #{k.inspect}"
        end

        r[k] = sql_value
      }

      return r
    end

    def parse_user(user)
      if user.nil?
        'NULL'
      elsif user.is_a?(Fixnum)
        user
      else
        raise "Unable to use user ID #{user.inspect}"
      end
    end
  end
end
