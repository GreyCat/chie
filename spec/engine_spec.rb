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

    it 'should raise an error trying to delete non-existing entity' do
      @e = Engine.new(CREDENTIALS)
      expect { @e.entity_delete('foo') }.to raise_error(NotFound)
    end

    it 'should be able to delete entity' do
      @e = Engine.new(CREDENTIALS)
      @e.entity_delete('book')
      cnt = 0
      @e.each_entity { cnt += 1 }
      expect(cnt).to eq(0)
    end
  end

  context 'creation of mixed indexable and non-indexable columns' do
    MIXED_SCHEMA = {
      'attr' => [
        {
          'name' => 'name',
          'type' => 'str',
          'len' => 100,
          'mand' => true,
          'ind' => true,
        },
        {
          'name' => 'int_non_ind',
          'type' => 'int',
          'mand' => false,
          'ind' => false,
        },
        {
          'name' => 'str_ind',
          'type' => 'str',
          'len' => 500,
          'mand' => false,
          'ind' => true,
        },
        {
          'name' => 'str_non_ind',
          'type' => 'str',
          'len' => 500,
          'mand' => false,
          'ind' => false,
        },
      ]
    }

    before(:all) do
      sqlclear
      @e = Engine.new(CREDENTIALS)
    end

    it 'should be able to create indexable and non-indexable columns' do
      ent = @e.entity_create(Entity.new('ent', MIXED_SCHEMA))

      ent2 = @e.entity('ent')
      expect(ent2).not_to be_nil

      r = sqldump.root.elements
      expect(r.to_a('//table_structure[@name="ent"]/field').map { |x| x.to_s }).to eq([
          "<field Comment='' Extra='auto_increment' Field='_id' Key='PRI' Null='NO' Type='int(11)'/>",
          "<field Comment='' Extra='' Field='_data' Key='' Null='YES' Type='mediumtext'/>",
          "<field Comment='' Extra='' Field='name' Key='' Null='YES' Type='varchar(100)'/>",
          "<field Comment='' Extra='' Field='str_ind' Key='' Null='YES' Type='varchar(500)'/>"
      ])
    end

    it 'should be able to insert data in all columns' do
      ent = @e.entity('ent')
      expect(ent).not_to be_nil
      ent.insert({
        'name' => 'Foo',
        'int_non_ind' => 42,
        'str_ind' => 'Bar',
        'str_non_ind' => 'Baz',
      })
    end

    it 'should be able to insert data in all columns' do
      ent = @e.entity('ent')
      expect(ent).not_to be_nil
      r = ent.get(1)
      expect(r).to eq({
        'name' => 'Foo',
        'int_non_ind' => 42,
        'str_ind' => 'Bar',
        'str_non_ind' => 'Baz',
      })
    end
  end

  context 'starting from empty database, connect by URL' do
    before(:all) do
      sqlclear
    end

    it 'should be able to connect' do
      @e = Engine.new(DATABASE_URL)
      expect(@e).not_to be_nil
    end
  end
end
