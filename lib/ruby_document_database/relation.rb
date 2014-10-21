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
  end
end
