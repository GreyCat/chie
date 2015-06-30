require 'spec_helper'

describe Record do
  describe '#==' do
    it 'returns true for two different objects with equal contents' do
      r1 = Record.new({'a' => 1, 'b' => 2})
      r2 = Record.new({'a' => 1, 'b' => 2})

      expect(r1 == r2).to eq(true)
    end

    it 'returns false for two different objects with different contents' do
      r1 = Record.new({'a' => 1, 'b' => 2})
      r2 = Record.new({'c' => 3})

      expect(r1 == r2).to eq(false)
    end
  end
end
