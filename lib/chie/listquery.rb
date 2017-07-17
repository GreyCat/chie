require 'chie/recordset'

module Chie
  class ListQuery
    attr_reader :fields, :tables, :where_phrase, :order_by

    def initialize(db, entity, opt = {})
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

      limit_phrase = ListQuery.parse_page_opts(@opt, @opt2)
      if limit_phrase
        q << limit_phrase
        @db.query("SELECT COUNT(*) AS cnt FROM #{tables} #{where_phrase}").each { |row|
          @opt2[:total_count] = row['cnt']
        }
      end

      return q
    end

    def self.parse_page_opts(opt, opt2)
      return nil unless opt[:page]

      per_page = opt[:per_page].to_i || 10
      opt2[:per_page] = per_page

      opt2[:page] = opt[:page].to_i
      opt2[:page] = 1 if opt2[:page] < 1
      " LIMIT #{(opt2[:page] - 1) * per_page}, #{per_page}"
    end

    def run
      qt = query
      begin
        q = @db.query(qt)
        RecordSet.new(q, @opt2)
      rescue Mysql2::Error => e
        raise "SQL error #{e.inspect} during query #{qt.inspect}"
      end
    end

    def count
      cnt = nil
      @db.query("SELECT COUNT(*) AS cnt FROM #{tables} #{where_phrase};").each { |row|
        cnt = row['cnt']
      }
      raise "Invalid query result returned from counting rows in #{@entity.name.inspect}" if cnt.nil?

      cnt
    end

    def group_count(group_by_name)
      r = {}
      @db.query("SELECT #{group_by_name} AS k, COUNT(*) AS cnt FROM `#{@entity.name}` #{where_phrase} GROUP BY #{group_by_name};").each { |row|
        r[row['k']] = row['cnt']
      }

      r
    end

    private

    def generate_fields
      @fields = @opt[:fields] || ['*']
      # TODO: do proper parsing of given field specifications, not just relay them directly into SQL

      @fields << @entity.sql_header_field

      # Make sure that main entity's JSON data column is always available
      @fields << "`#{@entity.name}`._data AS _data_0"

      @fields << "`#{@entity.name}`._id AS _id_0"

      # Check if we need to force joins
      @need_joins = false
      @entity.header.each { |a|
        @need_joins = true if a.is_a?(Array)
      }
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

      where = []
      opt_where.each_pair { |k, v|
        a = @entity.attr(k)
        r = @entity.rel(k)
        if a
          raise "Field #{k.inspect} is not indexed" unless a.indexed?
          wh = where_entry_single_column(k, v)
        elsif r
          if r.multi?
            @tables << " LEFT JOIN `#{r.sql_table}` ON `#{@entity.name}`.`_id`=`#{r.sql_table}`.`#{r.sql_column1}`"
            wh = where_entry_join_id(r, v)
          else
            wh = where_entry_single_column(k, v)
          end
        elsif k == '_id'
          # ID column is not an attribute, so it requires special case
          wh = where_entry_single_column(k, v)
        else
          raise "Invalid field name: #{k.inspect}" unless a
        end

        where << wh if wh
      } if opt_where

      unless @opt[:deleted]
        where << "`#{@entity.name}`._deleted=0"
      end

      @where_phrase = where.empty? ? '' : "WHERE #{where.join(' AND ')}"
    end

    def where_entry_single_column(k, v)
      # Try to convert value directly
      vv = @engine.escape_value(v)
      if not vv.nil?
        # if successful - it's an equality match against that value
        op = '='
      elsif v.is_a?(Range)
        # it's range match
        b1 = v.begin
        b2 = v.end
        if b1 == -Float::INFINITY and b2 == Float::INFINITY
          # (-inf..+inf) => no point doing any comparisons
          return nil
        elsif b1 == -Float::INFINITY
          # (-inf..b2]
          op = '<='
          vv = @engine.escape_value(b2)
        elsif b2 == Float::INFINITY
          # [b1..+inf)
          op = '>='
          vv = @engine.escape_value(b1)
        else
          # [b1..b2]
          op = 'BETWEEN'
          vv = "#{@engine.escape_value(b1)} AND #{@engine.escape_value(b2)}"
        end
      elsif v.is_a?(Array) and v.size == 2
        # otherwise try [operator, value] array match
        if v[0] == 'IN'
          raise "IN value #{v[1].inspect} is expected to be an array" unless v[1].is_a?(Array)

          # special handling of empty lists of valid values: it means
          # that condition should absolutely evaluate to "false"; "1=0"
          # is a quick and portable way SQL to specify "false"
          return '1=0' if v[1].empty?

          vv_arr = v[1].map { |x| @engine.escape_value(x) }
          op = 'IN'
          vv = "(#{vv_arr.join(',')})"
        else
          vv = @engine.escape_value(v[1])
          unless vv.nil?
            op = v[0]
          else
            raise "Unable to parse value in tuple condition #{v.inspect} for field #{k.inspect}"
          end
        end
      else
        raise "Unable to parse value #{v.inspect} for field #{k.inspect}"
      end

      "`#{@entity.name}`.`#{k}` #{op} #{vv}"
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
      return unless @opt[:resolve] or @need_joins

      @entity.each_rel { |r|
        raise "Unable to resolve in list (yet?) if multi-relations are present" if r.multi?

        # FIXME: probably it's wrong to just skip self-references, but
        # this solves self-linked table problem for now
        next if r.target == @entity.name

        @tables << " LEFT JOIN `#{r.target}` ON `#{@entity.name}`.`#{r.name}`=`#{r.target}`._id"
      }
    end

    def generate_order_by
      if @opt[:order_by]
        oo = @opt[:order_by]

        # Process single string element as well as arrays
        oo = [oo] unless oo.respond_to?(:join)

        @order_by = oo.map { |o|
          a = @entity.attr(o)
          if a
            # Attribute name - take care of proper escaping
            "`#{@entity.name}`.`#{a.name}`"
          else
            # Probably just an expression - leave it as is
            o.to_s
          end
        }.join(',')
      else
        @order_by = @entity.header.map { |x| fieldspec2sql(x) }.join(',')
      end
    end

    def fieldspec2sql(x)
      if x.is_a?(Array)
        rel, attr_name = x
        "`#{rel.target}`.`#{attr_name}`"
      else
        "`#{@entity.name}`.`#{x.name}`"
      end
    end
  end
end
