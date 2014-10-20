require 'spec_helper'

require 'mysql2'

SIMPLE_SCHEME = [
  {
    :name => 'name',
    :type => :str,
    :len => 100,
  },
  {
    :name => 'yr',
    :type => :int,
  },
]

describe Entity do
  before(:all) do
    sqlexec(CREDENTIALS, "DROP DATABASE #{CREDENTIALS[:database]}; CREATE DATABASE #{CREDENTIALS[:database]};")
    @e = Engine.new(CREDENTIALS)
    @e.entity_create('book', SIMPLE_SCHEME)
    @book = @e.entities['book']
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
end
