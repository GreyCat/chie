module RubyDocumentDatabase
  class Entity
    attr_reader :schema, :name

    def initialize(db, name, schema)
      @db = db
      @name = name
      @schema = schema

      @attr_by_name = {}

      @schema.each { |attr|
        k = attr['name']
        raise "Weird schema: no name in attribute #{attr.inspect}" unless k
        raise "Weird schema: duplicate attribute #{attr.inspect}" if @attr_by_name[k]
        @attr_by_name[k] = attr
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

    def to_json(opt)
      @schema.to_json(opt)
    end

    # ========================================================================

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

        sql_value = case attr['type']
        when 'str'
          "'#{@db.escape(v)}'"
        when 'int'
          v
        else
          raise "Invalid type #{attr[:type].inspect} encountered on attribute #{k.inspect}"
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
