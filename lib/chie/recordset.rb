module Chie
  class RecordSet
    def initialize(result, opt)
      @result = result
      @opt = opt
    end

    def each(&block)
      @result.each { |x| yield(x) }
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
  end
end
