require 'spec_helper'

require 'mysql2'

describe RecordSet do
  describe '#total_pages' do
    it 'should return single page if not pagination requested' do
      rs = RecordSet.new(nil, {})
      expect(rs.total_pages).to eq(1)
    end

    it 'should return 1 page if we have less than one page of records' do
      rs = RecordSet.new(nil, total_count: 3, per_page: 10, page: 1)
      expect(rs.total_pages).to eq(1)
    end

    it 'should return 1 page if we have exactly one page of records' do
      rs = RecordSet.new(nil, total_count: 10, per_page: 10, page: 1)
      expect(rs.total_pages).to eq(1)
    end

    it 'should return 2 pages if we have more than one page, but less than full two pages of records' do
      rs = RecordSet.new(nil, total_count: 11, per_page: 10, page: 1)
      expect(rs.total_pages).to eq(2)

      rs = RecordSet.new(nil, total_count: 13, per_page: 10, page: 1)
      expect(rs.total_pages).to eq(2)
    end

    it 'should return 2 pages if we have exactly 2 pages of records' do
      rs = RecordSet.new(nil, total_count: 20, per_page: 10, page: 1)
      expect(rs.total_pages).to eq(2)
    end
  end
end
