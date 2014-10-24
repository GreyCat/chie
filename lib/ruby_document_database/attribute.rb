module RubyDocumentDatabase
  class Attribute
    attr_reader :name, :type, :mandatory, :indexed, :len, :values

    def initialize(h)
      @name = h['name']
      raise "Invalid attribute #{h.inspect}: no name" unless @name
      @type = h['type']
      raise "Invalid attribute #{h.inspect}: no type" unless @type

      @title = h['title']

      @len = h['len']
      @values = h['values']

      @mandatory = h['mand'] || false
      @indexed = h['ind'] || false
    end

    def title
      @title || @name
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

      h.to_json(opt)
    end

    def as_sql_type
      case @type
      when 'str'
        len = @len || 256
        "VARCHAR(#{len})"
      when 'int'
        'INT'
      else
        raise "Invalid type #{@type.inspect} encountered on attribute #{@name.inspect}"
      end
    end

    def sql_value(db, v)
      case @type
      when 'str'
        "'#{db.escape(v)}'"
      when 'int'
        v
      else
        raise "Invalid type #{@type.inspect} encountered on attribute #{@name.inspect}"
      end
    end
  end
end
