module Chie
  ##
  # Implements lazy extraction of non-indexed attributes stored in
  # "_data" field of SQL record.
  class Record
    ##
    # Initializes a Chie record wrapper over a row from SQL
    # result. Typically a hash with several indexed fields, plus
    # "_data" field that includes all non-indexed fields in JSON form.
    def initialize(result)
      @result = result
      @data = nil
    end

    ##
    # Gets a single value from record by key. First checks in SQL
    # result fields directly (if that was the indexed field), then
    # tries to extract one from JSON "_data" column.
    def [](key)
      if @result.include?(key)
        @result[key]
      else
        data[key]
      end
    end

    ##
    # Gets a string sum of header fields for a particular record,
    # prepared in "_header" calculated field.
    def header
      @result['_header']
    end

    def data
      @data ||= JSON.load(@result['_data_0'] || @result['_data'])
    end

    def to_s
      data.to_s
    end

    def inspect
      {result: @result, json: data}.inspect
    end
  end
end
