require 'spec_helper'

describe Record do
  SIMPLE_PERSON_SCHEME = {
    'attr' => [
      {'name' => 'name', 'type' => 'str', 'ind' => true},
    ]
  }

  SIMPLE_BOOK_SCHEME = {
    'attr' => [
      {'name' => 'name', 'type' => 'str', 'len' => 100, 'ind' => true},
      {'name' => 'yr', 'type' => 'int', 'ind' => true},
      {'name' => 'comm', 'type' => 'str'},
    ],
    'rel' => [
      {'name' => 'author', 'target' => 'person', 'type' => '1'},
    ],
  }

  before(:all) do
    sqlclear
    @e = Engine.new(CREDENTIALS)
    @e.entity_create(Entity.new('person', SIMPLE_PERSON_SCHEME))
    @e.entity_create(Entity.new('book', SIMPLE_BOOK_SCHEME))

    @person_id = @e.entity!('person').insert({'name' => 'John Smith'})
    @book_id = @e.entity!('book').insert({'name' => 'Foo book', 'yr' => 4321, 'comm' => 'Blah', 'author' => @person_id})

    @person_rec = @e.entity!('person').get(@person_id)
    @book_rec = @e.entity!('book').get(@book_id)
  end

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

  describe '#id' do
    it 'gets database ID of a record' do
      expect(@book_rec.id).to eq(@book_rec_id)
    end
  end

  describe '#header' do
    it 'gets generated "header" field representation for a record' do
      expect(@book_rec.header).to eq('Foo book')
    end
  end

  describe '#[attr]' do
    it 'gets scalar value in indexed column' do
      expect(@book_rec['name']).to eq('Foo book')
    end

    it 'gets scalar value in non-indexed column' do
      expect(@book_rec['comm']).to eq('Blah')
    end
  end

  describe '#[rel]' do
  end
end
