module Chie
  class Attribute
    attr_reader :name, :type, :indexed, :len, :values, :unit, :opt

    def initialize(h)
      @name = h['name']
      raise "Invalid attribute #{h.inspect}: no name" unless @name
      @type = h['type']
      raise "Invalid attribute #{h.inspect}: no type" unless @type

      @title = h['title']

      @len = h['len']
      @values = h['values']
      @unit = h['unit']
      @opt = h['opt']

      @mandatory = h['mand'] || false
      @indexed = h['ind'] || false
    end

    def title
      @title || @name
    end

    def mandatory?
      @mandatory
    end

    def indexed?
      @indexed
    end

    # ========================================================================

    def to_json(opt)
      h = {
        'name' => @name,
        'type' => @type,
        'mand' => @mandatory,
        'ind' => @indexed,
      }

      h['title'] = @title if @title

      h['len'] = @len if @len
      h['values'] = @values if @values
      h['unit'] = @unit if @unit
      h['opt'] = @opt if @opt

      h.to_json(opt)
    end

    def as_sql_type
      case @type
      when 'str', 'password'
        len = @len || 256
        "VARCHAR(#{len})"
      when 'int', 'set', 'enum'
        'INT'
      when 'float'
        'DOUBLE'
      when 'text'
        'LONGTEXT'
      else
        raise "Invalid type #{@type.inspect} encountered on attribute #{@name.inspect}"
      end
    end

    def sql_value(db, v)
      return 'NULL' if v.nil?

      case @type
      when 'str', 'text', 'password'
        "'#{db.escape(v)}'"
      when 'int', 'set', 'enum', 'float'
        v
      else
        raise "Invalid type #{@type.inspect} encountered on attribute #{@name.inspect}"
      end
    end

    ##
    # Checks is a given data is considered empty in respect to
    # attribute's type. The check is different for every data type and
    # even non-existent for some. "data" is non-nil.
    def check_value_empty(data)
      case @type
      when 'str', 'text', 'password'
        data.empty?
      when 'int', 'float', 'enum'
        # These values are always non-empty
        false
      when 'set'
        data == 0
      else
        raise "Invalid type #{@type.inspect} encountered on attribute #{@name.inspect}"
      end
    end

    ##
    # Converts attribute value from internal stored format into
    # something more human-readable. Namely, "enum" values are
    # converted into relevant title strings and "set" values are
    # converted into arrays of title strings. Everything else is
    # passed "as is", as it should be already readable.
    def value_resolve(v)
      return nil if v.nil?

      case @type
      when 'str', 'text', 'int', 'float', 'password', 'bool'
        v
      when 'enum'
        @values[v]
      when 'set'
        res = []
        @values.each_with_index { |name, i|
          res << name if (v & (1 << i)) != 0
        }
        res
      else
        raise "Invalid type #{@type.inspect} encountered on attribute #{@name.inspect}"
      end
    end
  end
end
