require 'ruby_document_database/attribute'
require 'ruby_document_database/relation'

module RubyDocumentDatabase
  class ValidationError < Exception
    def initialize(errs)
      @errs = errs
    end

    def each(&block)
      @errs.each { |e| yield(e) }
    end
  end

  class NotFound < Exception; end

  class Entity
    attr_reader :name
    attr_accessor :db

    def initialize(name, h)
      Engine::validate_sql_name(name)

      @db = nil
      @name = name
      @title = h['title']
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

    def title
      @title || @name
    end

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

      basic_json = nil
      @db.query("SELECT _data FROM `#{@name}` WHERE _id=#{id};").each { |row|
        basic_json = row['_data']
      }
      raise NotFound.new("Invalid query result returned from getting data on ID=#{id}") if basic_json.nil?
      h = JSON.load(basic_json)
      resolve_relations(h)

      h
    end

    def list_by_name(query)
      query = @db.escape(query)
      @db.query("SELECT _id, name FROM `#{@name}` WHERE name LIKE '%#{query}%';")
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
      raise NotFound.new("Invalid query result returned from getting historical data on HID=#{hid}") if r.nil?

      h = JSON.load(r['_data'])
      h['_ts'] = r['ts']
      h['_user'] = r['user_id']
      resolve_relations(h)

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
      h = {}

      a = []
      each_attr { |v| a << v }
      h['attr'] = a unless a.empty?

      r = []
      each_rel { |v| r << v }
      h['rel'] = r unless r.empty?

      h['title'] = @title if @title

      h.to_json(opt)
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
      each_rel { |r|
        x = r.as_sql_type
        lines << x unless x.nil?
      }
      lines.join(', ')
    end

    private
    def validate_id(id)
      raise "ID must be integer, but got #{id.inspect}" unless id.is_a?(Fixnum)
    end

    def parse_data_with_schema(data)
      res = {
        '_data' => "'#{@db.escape(data.to_json)}'",
      }
      errs = []

      data.each_pair { |k, v|
        rel = @rel_by_name[k]
        attr = @attr_by_name[k]

        if attr
          # TODO: add string striping here

          if attr.mandatory and (v.nil? or v.empty?)
            errs << "Mandatory attribute #{k.inspect} is empty"
          end

          res[k] = attr.sql_value(db, v)
        elsif rel
          case rel.type
          when '01', '1'
            res[k] = v.to_i
          else
            raise InternalError.new("Unknown relation type #{rel.type}")
          end
        else
          errs << "Unknown argument #{k.inspect}"
        end
      }

      # Check that all mandatories are present
      each_attr { |a|
        errs << "Mandatory attribute #{a.name.inspect} is missing" if a.mandatory and res[a.name].nil?
      }
      each_rel { |rel|
        case rel.type
        when '01'
          # that's ok, it can be anything
        when '1'
          errs << "Mandatory relation #{rel.name.inspect} is missing" if res[rel.name].nil?
        else
          raise InternalError.new("Unknown relation type #{rel.type}")
        end
      }

      raise ValidationError.new(errs) unless errs.empty?

      return res
    end

    def resolve_relations(h)
      each_rel { |r|
        v = h[r.name]
        next if v.nil?

        # Wrap single scalar value in array, if it's type "01" or "1" relation
        v = [v] unless v.respond_to?(:join)

        # Resolve all related entities' IDs with names
        resolved = []
        @db.query("SELECT _id, name FROM `#{r.target}` WHERE _id IN (#{v.join(',')});").each { |row|
          resolved << {
            '_id' => row['_id'],
            'name' => row['name'],
          }
        }

        h[r.name] = resolved
      }
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
