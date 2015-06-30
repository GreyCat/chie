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
