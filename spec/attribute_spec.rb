require 'spec_helper'

describe Attribute do
  context 'of enum type' do
    before do
      @attr = Attribute.new({'name' => 'name', 'type' => 'enum', 'values' => ['foo', 'bar', 'baz']})
    end

    it 'converts to displayable properly' do
      expect(@attr.value_resolve(nil)).to be_nil
      expect(@attr.value_resolve(0)).to eq('foo')
      expect(@attr.value_resolve(2)).to eq('baz')
      expect(@attr.value_resolve(99)).to be_nil
    end
  end

  context 'of set type' do
    before do
      @attr = Attribute.new({'name' => 'name', 'type' => 'set', 'values' => ['foo', 'bar', 'baz']})
    end

    it 'converts to displayable properly' do
      expect(@attr.value_resolve(nil)).to be_nil
      expect(@attr.value_resolve(0)).to eq([])
      expect(@attr.value_resolve(2)).to eq(['bar'])
      expect(@attr.value_resolve(3)).to eq(['foo', 'bar'])
      expect(@attr.value_resolve(256)).to eq([])
    end
  end
end
