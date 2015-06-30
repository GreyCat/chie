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

  describe '#to_a' do
    it 'returns all rows as array, each row wrapped in Record' do
      result = [
        {'a' => 1, 'b' => 2},
        {'a' => 3, 'b' => 4},
      ]
      rs = RecordSet.new(result)
      rs_to_a = rs.to_a

      expect(rs_to_a.count).to eq(2)
      expect(rs_to_a[0]).to eq(Record.new({'a' => 1, 'b' => 2}))
      expect(rs_to_a[1]).to eq(Record.new({'a' => 3, 'b' => 4}))
    end
  end

  describe '#map' do
    it 'maps rows using given block' do
      result = [
        {'a' => 1, '_header' => 2},
        {'a' => 3, '_header' => 4},
      ]
      rs = RecordSet.new(result)
      rs_map = rs.map { |x| x['a'].header }

      expect(rs_map).to eq([2, 4])
    end
  end
end
