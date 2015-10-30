require 'chie/record'

module Chie
  class RecordSet
    def initialize(result, opt = {})
      @result = result
      @opt = opt
    end

    def each(&block)
      @result.each { |x| yield(Record.new(x)) }
    end

    def map(&block)
      @result.map { |x| yield(Record.new(x)) }
    end

    def to_a
      @result.map { |x| Record.new(x) }
    end

    def first
      r = @result.first
      r.nil? ? nil : Record.new(r)
    end

    def last
      r = @result.last
      r.nil? ? nil : Record.new(r)
    end

    def current_page
      @opt[:page]
    end

    def total_pages
      if @opt[:per_page]
        (@opt[:total_count] - 1) / @opt[:per_page] + 1
      else
        1
      end
    end

    def count
      @result.count
    end

    def total_count
      @opt[:total_count]
    end
  end
end
