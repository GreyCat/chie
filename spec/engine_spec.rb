require 'spec_helper'

require 'mysql2'

SIMPLE_SCHEMA = {
  'attr' => [
    {
      'name' => 'name',
      'type' => 'str',
      'len' => 100,
      'mand' => true,
      'ind' => true,
    },
    {
      'name' => 'yr',
      'type' => 'int',
      'mand' => false,
      'ind' => true,
    },
  ]
}

describe Engine do
  context 'starting from empty database' do
    before(:all) do
      sqlclear
      @e = Engine.new(CREDENTIALS)
    end

    it 'should connect to an empty MySQL database' do
      expect(@e).not_to be_nil
    end

    it 'should be able to create simple entity by given scheme' do
      book = @e.entity_create(Entity.new('book', SIMPLE_SCHEMA))

      expect(book).to be_kind_of(Entity)
      expect(@e.entity('book')).to eq(book)

      r = sqldump.root.elements
      expect(r.to_a('//table_structure[@name="book"]/field').map { |x| x.to_s }).to eq([
          "<field Comment='' Extra='auto_increment' Field='_id' Key='PRI' Null='NO' Type='int(11)'/>",
          "<field Comment='' Extra='' Field='_data' Key='' Null='YES' Type='mediumtext'/>",
          "<field Comment='' Extra='' Field='name' Key='' Null='YES' Type='varchar(100)'/>",
          "<field Comment='' Extra='' Field='yr' Key='' Null='YES' Type='int(11)'/>"
      ])
    end

    it 'should be able to see newly created entity' do
      expect(@e.entity('book')).not_to be_nil
      expect(JSON.load(@e.entity('book').to_json)).to eq(SIMPLE_SCHEMA)
    end

    it 'should be able to delete entity' do
      @e.entity_delete('book')
      expect(@e.entities).not_to include('book')
    end
  end

  context 'starting from existing database' do
    it 'should be able to initialize' do
      @e = Engine.new(CREDENTIALS)
    end

    it 'should be able to see newly created entity' do
      @e = Engine.new(CREDENTIALS)
      ent = @e.entity('book')
      expect(ent).not_to be_nil
      expect(JSON.load(ent.to_json)).to eq(SIMPLE_SCHEMA)
    end
  end
end
