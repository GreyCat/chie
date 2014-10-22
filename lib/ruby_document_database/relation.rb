module RubyDocumentDatabase
  class Relation
    attr_reader :name, :target, :type

    def initialize(h)
      @name = h['name']
      raise "Invalid relation #{h.inspect}: no name" unless @name
      @type = h['type']
      raise "Invalid relation #{h.inspect}: no type" unless @type
      @target = h['target']
      raise "Invalid relation #{h.inspect}: no target" unless @target
    end

    # ========================================================================

    def to_json(opt)
      h = {
        'name' => @name,
        'type' => @type,
        'target' => @target,
      }

      h.to_json(opt)
    end

    # Returns fragment of SQL table creation statement that would
    # create relevant single foreign key column, if applicable
    def as_sql_type
      case @type
      when '01'
        "#{@name} INT NULL, INDEX(#{@name})"
      when '1'
        "#{@name} INT NOT NULL, INDEX(#{@name})"
      end
    end
  end
end
