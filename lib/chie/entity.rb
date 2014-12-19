require 'chie/attribute'
require 'chie/relation'
require 'chie/recordset'

module Chie
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
      parse_header(h['header'])
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

    ##
    # Parses header fields array, replacing attribute names with
    # references to real attribute objects.
    def parse_header(h_header)
      if h_header.nil?
        attr = @attr_by_name['name']
        raise "Weird schema: entity must include attribute \"name\" or specify alternative header fields" unless attr
        @header = [attr]
      else
        @header = h_header.map { |a|
          attr = @attr_by_name[a]
          raise "Weird schema: header field includes attribute #{a.inspect}, but it doesn't exist" unless attr
          attr
        }
      end
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

    attr_reader :attrs, :rels, :header

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

      @db.query("SELECT _data FROM `#{@name}` WHERE _id=#{id};").each { |row|
        basic_json = row['_data']
        h = JSON.load(basic_json)
        resolve_relations(h)
        return h
      }

      raise NotFound.new("Invalid query result returned from getting data on ID=#{id}")
    end

    ##
    # Lists a collection of records of this entity, returning an
    # iterable collection of rows, each containing fields ['_id'] and
    # ['name'].  Default invocation lists all possible
    # records. Additional options, specified in `opt` hash can be used
    # to filter output:
    #
    # * :fields - fields to request; by default, requests all fields
    #   ("*")
    # * :where - where phrase
    # * :per_page - number of records to output on one page, used in
    #   conjuction with `:page` parameter; if not specified, default
    #   value is 10.
    # * :page - output only records on specified page of pages, each
    #   containing `per_page` records; first page is #1.
    def list(opt = {})
      fields = opt[:fields] || ['*']

      # Header field handling
      if header.size == 1
        header_exp = "`#{header.first.name}`"
      else
        header_fields = header.map { |a| "`#{a.name}`" }.join(",' ',")
        header_exp = "CONCAT(#{header_fields})"
      end
      fields << "#{header_exp} AS _header"

      q = "SELECT #{fields.join(',')} FROM `#{@name}`"

      if opt[:where] and not opt[:where].empty?
        where = []
        opt[:where].each_pair { |k, v|
          a = @attr_by_name[k]
          r = @rel_by_name[k]
          if a
            raise "Field #{k.inspect} is not indexed" unless a.indexed?
          elsif r
            raise "Unable to match against multi-relation #{k.inspect}" if r.multi?
          else
            raise "Invalid field name: #{k.inspect}" unless a
          end

          # Try to convert value directly
          vv = escape_value(v)
          if not vv.nil?
            # if successful - it's an equality match against that value
            op = '='
          elsif v.is_a?(Array) and v.size == 2
            # otherwise try [operator, value] array match
            vv = escape_value(v[1])
            unless vv.nil?
              op = v[0]
            else
              raise "Unable to parse value in tuple condition #{v.inspect} for field #{k.inspect}"
            end
          else
            raise "Unable to parse value #{v.inspect} for field #{k.inspect}"
          end

          where << "`#{k}` #{op} #{vv}"
        }
        q << ' WHERE '
        q << where.join(' AND ')
      end

      order_by = header.map { |x| "`#{x.name}`" }.join(', ')
      q << " ORDER BY #{order_by}"
      opt2 = {}

      if opt[:page]
        per_page = opt[:per_page].to_i || 10
        opt2[:per_page] = per_page

        @db.query("SELECT COUNT(*) AS cnt FROM `#{@name}`").each { |row|
          opt2[:total_count] = row['cnt']
        }

        opt2[:page] = opt[:page].to_i
        opt2[:page] = 1 if opt2[:page] < 1
        q << " LIMIT #{(opt2[:page] - 1) * per_page}, #{per_page}"
      end

      RecordSet.new(@db.query(q), opt2)
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
      h['_ts'] = Time.at(r['ts'])
      h['_user'] = r['user_id']
      resolve_relations(h)

      h
    end

    ##
    # Inserts new record.
    #
    # @param [Hash] data record to be inserted
    # @param [Fixnum, nil] user ID of user that does this operation; by default the user is nil and thus the operation is considered anonymous.
    # @param [Time, nil] time timestamp of operation; by default, current time would be used
    # @raise [Chie::ValidationError] if given record has missing or empty mandatory fields
    # @return [Fixnum] ID of inserted record
    def insert(data, user = nil, time = nil)
      check_mandatories(data)
      cols = generate_sql_columns(data)

      real_insert(data, cols, user, time)
    end

    ##
    # Replaces the record with a given id with the data in supplied hash.
    # @param [Fixnum, nil] user ID of user that does this operation; by default the user is nil and thus the operation is considered anonymous.
    # @param [Time, nil] time timestamp of operation; by default, current time would be used
    def update(id, data, user = nil, time = nil)
      validate_id(id)

      check_mandatories(data)
      cols = generate_sql_columns(data)
      cols['_id'] = id

      # Check if exactly the same data is already in the database; if it is, don't do any writes
      exist_json = nil
      # TODO: start transaction here
      @db.query("SELECT _data FROM `#{@name}` WHERE _id=#{id};").each { |row|
        exist_json = row['_data']
      }
      if data.to_json != exist_json
        # Delete original record
        @db.query("DELETE FROM `#{@name}` WHERE _id=#{id};")

        # Delete multi relations
        each_rel { |r|
          @db.query("DELETE FROM `#{r.name}` WHERE `#{@name}`=#{id};") if r.multi?
        }

        real_insert(data, cols, user, time)
      end
      # TODO: end transaction here
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
        lines << "#{a.name} #{a.as_sql_type}" if a.indexed?
      }
      each_rel { |r|
        x = r.as_sql_type
        lines << x unless x.nil?
      }
      lines.join(', ')
    end

    private
    ##
    # Performs the actual insert of prepared columns into relevant
    # tables. This code is shared between `insert` and `update`
    # operations to make sure it always does the same, no matter what
    # was the operation.
    #
    # @param [Hash] data data to be inserted; would be used only to
    # perform additional inserts for multi relations.
    # @param [Hash<String, String>] cols prepared string-to-string
    # key-value pairs with column names and relevant string content
    # (already quoted, if required).
    # @param [Fixnum] user ID of user that does this operation; by default the user is nil and thus the operation is considered anonymous.
    # @param [Time] time timestamp of operation; if nil, current time would be used.
    #
    # @return [Fixnum] ID of inserted record
    def real_insert(data, cols, user, time)
      user = parse_user(user)
      time = time_to_mysql(time)

      col_names = cols.keys.map { |x| "`#{x}`" }.join(',')
      col_vals = cols.values.join(',')

      # Main table
      @db.query("INSERT INTO `#{@name}` (#{col_names}) VALUES (#{col_vals});")
      id = @db.last_id

      # History table
      @db.query("INSERT INTO `#{@name}_h` (_id, _data, ts, user_id) VALUES (#{id}, #{cols['_data']}, #{time}, #{user});")

      # Relation link tables
      each_rel { |r|
        if r.multi?
          vv = data[r.name]
          vv.each { |v|
            @db.query("INSERT INTO `#{r.name}` (`#{@name}`, `#{r.target}`) VALUES (#{id}, #{v});")
          } unless vv.nil?
        end
      }

      id
    end

    def validate_id(id)
      raise "ID must be integer, but got #{id.inspect}" unless id.is_a?(Fixnum)
    end

    def check_mandatories(data)
      errs = []

      each_attr { |a|
        next unless a.mandatory?
        if data[a.name].nil?
          errs << "Mandatory attribute #{a.name.inspect} is missing" 
        else
          errs << "Mandatory attribute #{a.name.inspect} is empty" if a.check_value_empty(data[a.name])
        end
      }

      each_rel { |r|
        next unless r.mandatory?
        v = data[r.name]
        if v.nil?
          errs << "Mandatory relation #{r.name.inspect} is missing"
        else
          if r.multi?
            errs << "Mandatory relation #{r.name.inspect} is empty" if v.empty?
          else
            # we don't check types and non-nil object seems to be ok in any case
          end
        end
      }

      raise ValidationError.new(errs) unless errs.empty?
    end

    ##
    # Prepares a hash with keys named as SQL columns and values with
    # data that should be inserted into each of them. Normally it
    # would include `_data` column with all the data serialized as
    # JSON and individual columns corresponding to indexed attributes
    # and relations separately.
    def generate_sql_columns(data)
      res = {
        '_data' => "'#{@db.escape(data.to_json)}'",
      }

      data.each_pair { |k, v|
        rel = @rel_by_name[k]
        attr = @attr_by_name[k]

        if attr
          # Only add indexed attributes; non-indexed would be normally available via _data
          res[k] = attr.sql_value(db, v) if attr.indexed?
        elsif rel
          # Multi relations would be registered in separate n-to-n table, not as a column here
          res[k] = v.to_i if not rel.multi?
        else
          raise ArgumentError.new("Unknown argument #{k.inspect}")
        end
      }

      return res
    end

    def resolve_relations(h)
      each_rel { |r|
        v = h[r.name]
        next if v.nil? or (v.respond_to?(:empty?) and v.empty?)

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

    def time_to_mysql(time)
      time = Time.now if time.nil?
      time.to_i
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
  end
end
