module Chie
  class Relation
    attr_reader :entity, :name, :target, :type

    def initialize(entity, h)
      @entity = entity

      @name = h['name']
      raise "Invalid relation #{h.inspect}: no name" unless @name
      @type = h['type']
      raise "Invalid relation #{h.inspect}: no type" unless @type
      @target = h['target']
      raise "Invalid relation #{h.inspect}: no target" unless @target

      @title = h['title']
    end

    # ========================================================================

    def title
      @title || @name
    end

    def mandatory?
      @type == '1' or @type == '1n'
    end

    def multi?
      @type == '0n' or @type == '1n'
    end

    def to_json(opt)
      h = {
        'name' => @name,
        'type' => @type,
        'target' => @target,
      }

      h['title'] = @title if @title

      h.to_json(opt)
    end

    def sql_table
      @sql_table ||= @name
    end

    def sql_column1
      calc_sql_names unless @sql_column1
      @sql_column1
    end

    def sql_column2
      calc_sql_names unless @sql_column2
      @sql_column2
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

    private
    def calc_sql_names
      if @entity.name == @target
        @sql_column1 = "#{@entity.name}_1"
        @sql_column2 = "#{@entity.name}_2"
      else
        @sql_column1 = @entity.name
        @sql_column2 = @target
      end
    end
  end
end
