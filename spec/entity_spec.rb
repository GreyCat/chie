require 'spec_helper'

require 'mysql2'

SIMPLE_SCHEME = {
  'attr' => [
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
}

describe Entity do
  before(:all) do
    sqlclear
    @e = Engine.new(CREDENTIALS)
    @book = Entity.new('book', SIMPLE_SCHEME)
    @e.entity_create(@book)
  end

  SIMPLE_RECORD = {
    'name' => 'Lorem ipsum',
    'yr' => 1234,
  }

  SIMPLE_RECORD_2 = {
    'name' => 'Dolor sit amet',
    'yr' => 4321,
  }

  it 'should be able to properly convert data hash to SQL according to the schema' do
    expect(@book.instance_eval { parse_data_with_schema(SIMPLE_RECORD) }).to eq({
        '_data' => "'{\\\"name\\\":\\\"Lorem ipsum\\\",\\\"yr\\\":1234}'",
        'name' => "'Lorem ipsum'",
        'yr' => 1234,
    })

    # FIXME: details about exception
    expect { @book.instance_eval { parse_data_with_schema({'foo' => 1234}) } }.to raise_error
  end

  it 'should be able to insert simple record in a given entity anonymously' do
    id = @book.insert(SIMPLE_RECORD)
    expect(id).to eq(1)
    expect(@book.count).to eq(1)
  end

  it 'should be able to retrieve inserted record' do
    rec = @book.get(1)
    expect(rec).to eq(SIMPLE_RECORD)
  end

  it 'should be able to modify record' do
    @book.update(1, SIMPLE_RECORD_2)
    rec = @book.get(1)
    expect(rec).to eq(SIMPLE_RECORD_2)
  end

  it 'should be able to see two distinct versions of our record' do
    hist = @book.history_list(1)
    expect(hist.size).to eq(2)
  end

  it 'should be able to get record in version #1' do
    hist = @book.history_get(1)
    expect(hist['name']).to eq(SIMPLE_RECORD['name'])
    expect(hist['yr']).to eq(SIMPLE_RECORD['yr'])
    expect(hist['_ts']).to be_kind_of(Time)
  end

  context 'book->source relation' do
    SOURCE_SCHEME = {
      'attr' => [
        {
          'name' => 'name',
          'type' => 'str',
          'len' => 100,
        }
      ]
    }

    ARTICLE_SCHEME = {
      'attr' => [
        {
          'name' => 'name',
          'type' => 'str',
          'len' => 100,
        }
      ],
      'rel' => [
        {
          'name' => 'source',
          'target' => 'source',
          'type' => '1',
        }
      ]
    }

    before(:all) do
      sqlclear
      @e = Engine.new(CREDENTIALS)
    end

    it 'should be able to create two related entities' do
      @source = @e.entity_create(Entity.new('source', SOURCE_SCHEME))
      @article = @e.entity_create(Entity.new('article', ARTICLE_SCHEME))

      expect(@e.entity('source')).to eq(@source)
      expect(@e.entity('article')).to eq(@article)
    end

    it 'should be able to insert record in free entity' do
      @source = @e.entity('source')
      @source.insert('name' => 'Source')
      expect(@source.count).to eq(1)
    end
  end
end
