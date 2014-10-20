require 'spec_helper'

require 'mysql2'

SIMPLE_SCHEMA = [
  {
    'name' => 'name',
    'type' => 'str',
    'len' => 100,
  },
  {
    'name' => 'yr',
    'type' => 'int',
  },
]

describe Engine do
  context 'starting from empty database' do
    before(:all) do
      sqlexec(CREDENTIALS, "DROP DATABASE #{CREDENTIALS[:database]}; CREATE DATABASE #{CREDENTIALS[:database]};")
      @e = Engine.new(CREDENTIALS)
    end

    it 'should connect to an empty MySQL database' do
      expect(@e).not_to be_nil
      expect(@e.entities).to eq({})
    end

    it 'should be able to create simple entity by given scheme' do
      book = @e.entity_create('book', SIMPLE_SCHEMA)

      expect(book).to be_kind_of(Entity)
      expect(@e.entities.keys).to eq(['book'])
      expect(@e.entities['book']).to eq(book)

      r = sqldump(CREDENTIALS).root.elements
      #    expect(r.to_a('//table_structure[@name="book"]/field')map { |x| x.to_s }.join).to eq("xxx")
      expect(r.to_a('//table_structure[@name="book"]/field').to_s).to eq("xxx")
    end

    it 'should be able to see newly created entity' do
      expect(@e.entities['book']).not_to be_nil
      expect(@e.entities['book'].schema).to eq(SIMPLE_SCHEMA)
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
      expect(@e.entities['book']).not_to be_nil
      expect(@e.entities['book'].schema).to eq(SIMPLE_SCHEMA)
    end
  end
end
