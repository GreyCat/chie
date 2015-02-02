require 'chie/recordset'

module Chie
  class ListQuery
    attr_reader :fields, :tables, :where_phrase, :order_by

    def initialize(db, entity, opt)
      @db = db
      @entity = entity
      @opt = opt

      @engine = @entity.engine

      generate_fields
      generate_tables
      generate_where_phrase
      generate_order_by
    end

    def query
      q = "SELECT #{fields.join(',')} FROM #{tables} #{where_phrase} ORDER BY #{order_by}"

      @opt2 = {}

      if @opt[:page]
        per_page = @opt[:per_page].to_i || 10
        @opt2[:per_page] = per_page

        @db.query("SELECT COUNT(*) AS cnt FROM #{tables} #{where_phrase}").each { |row|
          @opt2[:total_count] = row['cnt']
        }

        @opt2[:page] = @opt[:page].to_i
        @opt2[:page] = 1 if opt2[:page] < 1
        q << " LIMIT #{(@opt2[:page] - 1) * per_page}, #{per_page}"
      end

      return q
    end

    def run
      q = @db.query(query)
      RecordSet.new(q, @opt2)
    end

    def count
      cnt = nil
      @db.query("SELECT COUNT(*) AS cnt FROM `#{@entity.name}` #{where_phrase};").each { |row|
        cnt = row['cnt']
      }
      raise "Invalid query result returned from counting rows in #{@entity.name.inspect}" if cnt.nil?

      cnt
    end

    private

    def generate_fields
      @fields = @opt[:fields] || ['*']
      @fields << @entity.sql_header_field
    end

    ##
    # Returns SQL WHERE phrase (including WHERE keyword) that will
    # filter SELECT using given conditions.
    #
    # @param [Hash] opt_where option hash that describes search
    #   condition; all keys in this hash are supposed to be attribute
    #   names to be matched; values can take one of two forms - either
    #   an immediate value (which results in equality matching for
    #   that value) or an [operator; value] tuple (which results in
    #   "attribute operator value" clause); all options are always
    #   connected with AND operators.
    # @return [String] WHERE phrase to fulfill given search condition
    #   request
    def generate_where_phrase
      opt_where = @opt[:where]

      return '' if opt_where.nil? or opt_where.empty?

      where = []
      opt_where.each_pair { |k, v|
        a = @entity.attr(k)
        r = @entity.rel(k)
        if a
          raise "Field #{k.inspect} is not indexed" unless a.indexed?
          where << where_entry_single_column(k, v)
        elsif r
          if r.multi?
            @tables << " LEFT JOIN `#{r.sql_table}` ON `#{@entity.name}`.`_id`=`#{r.sql_table}`.`#{r.sql_column1}`"
            where << where_entry_join_id(r, v)
          else
            where << where_entry_single_column(k, v)
          end
        else
          raise "Invalid field name: #{k.inspect}" unless a
        end
      }

      @where_phrase = "WHERE #{where.join(' AND ')}"
    end

    def where_entry_single_column(k, v)
      # Try to convert value directly
      vv = @engine.escape_value(v)
      if not vv.nil?
        # if successful - it's an equality match against that value
        op = '='
      elsif v.is_a?(Array) and v.size == 2
        # otherwise try [operator, value] array match
        vv = @engine.escape_value(v[1])
        unless vv.nil?
          op = v[0]
        else
          raise "Unable to parse value in tuple condition #{v.inspect} for field #{k.inspect}"
        end
      else
        raise "Unable to parse value #{v.inspect} for field #{k.inspect}"
      end

      "`#{k}` #{op} #{vv}"
    end

    def where_entry_join_id(r, v)
      vv = @engine.escape_value(v)
      raise "Unable to parse value for multi-relation join ID" unless vv
      "`#{r.sql_table}`.`#{r.sql_column2}` = #{vv}"
    end

    ##
    # Returns SQL tables expression. If @opt[:resolve] is true, joins
    # in all related entities.
    def generate_tables
      @tables = "`#{@entity.name}`"
      return unless @opt[:resolve]

      @entity.each_rel { |r|
        raise "Unable to resolve in list (yet?) if multi-relations are present" if r.multi?
        @tables << " LEFT JOIN `#{r.target}` ON `#{@name}`.`#{r.name}`=`#{r.target}`._id"
      }
    end

    def generate_order_by
      @order_by = @entity.header.map { |x| "`#{@entity.name}`.`#{x.name}`" }.join(', ')
    end
  end
end
