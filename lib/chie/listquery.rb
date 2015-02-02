require 'chie/recordset'

module Chie
  class ListQuery
    def initialize(db, entity, opt)
      @db = db
      @entity = entity
      @opt = opt

      @engine = @entity.engine
    end

    def query
      tables = list_tables(@opt[:resolve])
      where_phrase = list_where_phrase(@opt[:where])
      order_by = @entity.header.map { |x| "`#{@entity.name}`.`#{x.name}`" }.join(', ')

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
      where_phrase = list_where_phrase(@opt[:where])

      cnt = nil
      @db.query("SELECT COUNT(*) AS cnt FROM `#{@entity.name}` #{where_phrase};").each { |row|
        cnt = row['cnt']
      }
      raise "Invalid query result returned from counting rows in #{@entity.name.inspect}" if cnt.nil?

      cnt
    end

    def fields
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
    private
    def list_where_phrase(opt_where)
      return '' if opt_where.nil? or opt_where.empty?

      where = []
      opt_where.each_pair { |k, v|
        a = @entity.attr(k)
        r = @entity.rel(k)
        if a
          raise "Field #{k.inspect} is not indexed" unless a.indexed?
        elsif r
          raise "Unable to match against multi-relation #{k.inspect}" if r.multi?
        else
          raise "Invalid field name: #{k.inspect}" unless a
        end

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

        where << "`#{k}` #{op} #{vv}"
      }

      "WHERE #{where.join(' AND ')}"
    end

    ##
    # Returns SQL tables expression.
    #
    # @param resolve Joins in all related entities, if true.
    def list_tables(resolve)
      return "`#{@entity.name}`" unless resolve

      t = "`#{@entity.name}`"

      @entity.each_rel { |r|
        raise "Unable to resolve in list (yet?) if multi-relations are present" if r.multi?
        t << " LEFT JOIN `#{r.target}` ON `#{@name}`.`#{r.name}`=`#{r.target}`._id"
      }

      t
    end
  end
end
