module RubyDocumentDatabase
  class Attribute
    attr_reader :name, :type, :mandatory, :indexed

    def initialize(h)
      @name = h['name']
      raise "Invalid attribute #{h.inspect}: no name" unless @name
      @type = h['type']
      raise "Invalid attribute #{h.inspect}: no type" unless @type

      @len = h['len']

      @mandatory = h['mand'] || false
      @indexed = h['ind'] || false
    end

    # ========================================================================

    def to_json(opt)
      h = {
        'name' => @name,
        'type' => @type,
        'mand' => @mandatory,
        'ind' => @indexed,
      }

      h['len'] = @len if @len

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
  end
end
