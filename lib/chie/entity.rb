require 'chie/attribute'
require 'chie/relation'
require 'chie/listquery'
require 'chie/searchquery'

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
  class TooManyFound < Exception; end
  class InvalidSchema < Exception; end

  class Entity
    attr_reader :name
    attr_accessor :db
    attr_accessor :engine

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
        raise InvalidSchema.new("duplicate attribute #{attr.inspect}") if @attr_by_name[attr.name]
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
        raise InvalidSchema.new("entity must include attribute \"name\" or specify alternative header fields") unless attr
        @header = [attr]
      else
        @header = h_header.map { |a|
          attr = @attr_by_name[a]
          raise InvalidSchema.new("header field includes attribute #{a.inspect}, but it doesn't exist") unless attr
          attr
        }
      end
    end

    def parse_rel(h_rel)
      @rels = []
      @rel_by_name = {}

      h_rel.each { |r|
        rel = Relation.new(self, r)
        raise InvalidSchema.new("duplicate relation name #{rel.inspect}") if @rel_by_name[rel.name]
        @rel_by_name[rel.name] = rel
        @rels << rel.name
      }
    end

    # ========================================================================

    attr_reader :attrs, :rels, :header

    def title
      @title || @name
    end

    ##
    # Gets attribute by name in this entity.
    # @param [String] name name of the attribute
    # @return [Attribute, nil] attribute if it exists, nil otherwise
    def attr(name)
      @attr_by_name[name]
    end

    ##
    # Gets attribute by name in this entity. Fails with an exception
    # if requested attribute does not exist.
    # @param [String] name name of the attribute
    # @return [Attribute] attribute
    def attr!(name)
      @attr_by_name[name] or raise NotFound.new("Attribute #{name.inspect} not found")
    end

    ##
    # Gets relation by name in this entity.
    # @param [String] name name of the relation
    # @return [Relation, nil] relation if it exists, nil otherwise
    def rel(name)
      @rel_by_name[name]
    end

    ##
    # Gets relation by name in this entity. Fails with an exception if
    # requested relation does not exist.
    # @param [String] name name of the relation
    # @return [Relation] relation
    def rel!(name)
      @rel_by_name[name] or raise NotFound.new("Relation #{name.inspect} not found")
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

    def each_rel_back(&block)
      @engine.each_entity { |e|
        e.each_rel { |r|
          yield r if r.target == @name
        }
      }
    end

    # ========================================================================

    ##
    # Counts number of records in collection. An optional hash with
    # conditions can be passed to count a number of records that match
    # given condition.
    #
    # @see Entity#list
    # @see ListQuery
    def count(opt = {})
      q = ListQuery.new(@db, self, opt)
      q.count
    end

    ##
    # Counts number of records in collection, grouped by a certain
    # attribute or single-type relation. An optional hash can be
    # passed to count only records that match given condition.
    #
    # @param [String] group_by_name the name of a field (attribute or
    # relation) perform a group by on
    # @return [Hash] key-value pairs, where keys are all possible
    # values of group-by attribute or relation, and values are integer
    # quantities of records existing for this group-by key
    #
    # @see Entity#list
    # @see ListQuery
    def group_count(group_by_name, opt = {})
      q = ListQuery.new(@db, self, opt)
      q.group_count(group_by_name)
    end

    def get(id)
      validate_id(id)

      @db.query("SELECT _data, #{sql_header_field} FROM `#{@name}` WHERE _id=#{id};").each { |row|
        basic_json = row['_data']
        h = JSON.load(basic_json)
        h['_header'] = row['_header']
        resolve_relations(h)
        return h
      }

      raise NotFound.new("Invalid query result returned from getting data on ID=#{id}")
    end

    ##
    # Finds exactly one record that satisfies given where phrase.
    #
    # @return [Record, nil] first one of all the records found or nil
    # if no records were found
    def find_by(where)
      list(where: where).first
    end

    ##
    # Finds exactly one record that satisfies given where phrase.
    # Throws an expection if no records were found or more than 1
    # record satisfies given where phrase.
    #
    # @return [Record] a record
    def find_by!(where)
      r = list(where: where)
      case r.count
      when 0
        raise NotFound.new("No record found that matches #{where.inspect}")
      when 1
        return r.first
      else
        raise TooManyFound.new("Too many records satisfy #{where.inspect}")
      end
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
    # * :order_by - "order by" specification; a single string or array
    #   of strings; each string may be the name of attribute of
    #   current entity (in this case it will get properly escaped) or
    #   just be an arbitrary SQL expression (in this case it will be
    #   used as is)
    # * :per_page - number of records to output on one page, used in
    #   conjuction with `:page` parameter; if not specified, default
    #   value is 10.
    # * :page - output only records on specified page of pages, each
    #   containing `per_page` records; first page is #1.
    # * :resolve - if true, resolve (join) all related entities,
    #   allowing access to their fields
    def list(opt = {})
      q = ListQuery.new(@db, self, opt)
      q.run
    end

    ##
    # Searches records of this entity, pulling along all related
    # entries, joining the tables as necessary. Options can be used to
    # specify what should be searched, joined in and returned:
    #
    # @param [Hash] opt search options
    # @option opt [Array<String>] fields
    # @option opt [Array<Array<String>>] where
    # @option opt [Array<String>] order
    # TODO
    #
    # @return [RecordSet] an iterable collection of records with
    # requested fields filled
    def search(opt = {})
      q = SearchQuery.new(@db, self, opt)
      q.run
    end

    def list_by_name(query)
      query = @db.escape(query)
      @db.query("SELECT _id, name FROM `#{@name}` WHERE name LIKE '%#{query}%';")
    end

    ##
    # Gets a list of historical states of a given record. Default
    # invocation lists all possible records.
    #
    # @param [Fixnum] id identifier of a record to investigate
    # @param [Hash] opt additional options to filter output
    # @option opt [Fixnum] (nil) :page output only records on
    # specified page of pages, each containing `per_page` records;
    # first page is #1; disabled by default, thus returning all
    # records
    # @option opt [Fixnum] (10) :per_page number of records to output
    # on one page, used only if `:page` parameter is used
    def history_list(id, opt = {})
      validate_id(id)

      opt2 = {}
      limit_phrase = ListQuery.parse_page_opts(opt, opt2)

      @db.query("SELECT hid, ts, user_id FROM `#{@name}_h` WHERE _id=#{id}#{limit_phrase};")
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
      canonicalize_data(data)
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

      canonicalize_data(data)
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
          @db.query("DELETE FROM `#{r.sql_table}` WHERE `#{r.sql_column1}`=#{id};") if r.multi?
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

      # Store "header" if it's non-default
      unless @header.size == 1 && @header.first == @attr_by_name['name']
        h['header'] = @header.map { |x| x.name }
      end

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
        lines << "UNIQUE INDEX `_uniq_#{a.name}` (#{a.name})" if a.unique?
      }
      each_rel { |r|
        x = r.as_sql_type
        lines << x unless x.nil?
      }
      lines.join(', ')
    end

    ##
    # Returns SQL SELECT expression for a special column that would
    # represent all header fields properly concatenated using SQL
    # server syntax.
    def sql_header_field
      if header.size == 1
        header_exp = "`#{@name}`.`#{header.first.name}`"
      else
        header_fields = header.map { |a| "`#{@name}`.`#{a.name}`" }.join(",' ',")
        header_exp = "CONCAT(#{header_fields})"
      end
      "#{header_exp} AS _header"
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
            @db.query("INSERT INTO `#{r.sql_table}` (`#{r.sql_column1}`, `#{r.sql_column2}`) VALUES (#{id}, #{v});")
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

        tgt_ent = @engine.entity(r.target)

        # Wrap single scalar value in array, if it's type "01" or "1" relation
        v = [v] unless v.respond_to?(:join)

        # Resolve all related entities' IDs with names
        resolved = []
        @db.query("SELECT _id, #{tgt_ent.sql_header_field} FROM `#{r.target}` WHERE _id IN (#{v.join(',')});").each { |row|
          resolved << {
            '_id' => row['_id'],
            '_header' => row['_header'],
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
    # Converts data hash from "presentation" form (with lots of
    # resolved links and extra information) to "canonical" form (terse
    # and basic).
    protected
    def canonicalize_data(data)
      # "_header" is a synthetic field, usually derived from other fields
      data.delete('_header')

      each_rel { |r|
        vv = data[r.name]
        if r.multi?
          unless vv.nil?
            raise ArgumentError.new("Relation #{r.name} is multi, expected enumerable for value, got #{vv.inspect}") unless vv.respond_to?(:map!)
            vv.map! { |v| parse_presentation_id(v, r) }
          end
          # Empty array is non-canonical; if we've got empty array,
          # then delete whole key for good
          data.delete(r.name) if vv.nil? or vv.empty?
        else
          # Canonical form is a single integer ID
          # Presentation form could be an array of single value
          if vv.is_a?(Array)
            case vv.size
            when 0
              # No value; just delete whole key
              data.delete(r.name)
            when 1
              # Remove array wrapping
              v = vv.first
              v = parse_presentation_id(v, r)
              data[r.name] = v
            else
              raise ArgumentError.new("Relation #{r.name} is single, but got #{vv.size} values")
            end
          else
            # Normal, single value
            if vv.nil?
              data.delete(r.name)
            else
              data[r.name] = parse_presentation_id(vv, r)
            end
          end
        end
      }
    end

    ##
    # Canonical form of relation ID is integer IDs; presentation form
    # could be a hashes that contain at least '_id' and '_header' keys.
    def parse_presentation_id(v, r)
      if v.is_a?(Hash)
        id = v['_id']
        raise ArgumentError.new("Unable to parse value for relation #{r.name}: #{v.inspect}") if id.nil?
      else
        id = v
      end
      raise ArgumentError.new("Invalid type in value for relation #{r.name}: expected integer, got #{id.inspect}") unless id.is_a?(Integer)
      id
    end
  end
end
