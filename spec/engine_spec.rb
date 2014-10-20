require 'spec_helper'

require 'mysql2'

CREDENTIALS = {
  :host => 'localhost',
  :username => 'rdd_test',
  :password => 'rdd_test',
  :database => 'rdd_test',
}

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

describe Engine do
  before(:all) do
    sqlexec(CREDENTIALS, "DROP DATABASE #{CREDENTIALS[:database]}; CREATE DATABASE #{CREDENTIALS[:database]};")
    @e = Engine::connect_mysql(CREDENTIALS)
  end

  it 'should connect to an empty MySQL database' do
    expect(@e).not_to be_nil
    expect(@e.desc).to eq({
      'version' => 1,
      'entities' => {},
    })
  end

  it 'should be able to create simple entity by given scheme' do
    @e.entity_create('book', SIMPLE_SCHEME)
    expect(@e.desc['entities'].keys).to eq(['book'])

    r = sqldump(CREDENTIALS).root.elements
#    expect(r.to_a('//table_structure[@name="book"]/field')map { |x| x.to_s }.join).to eq("xxx")
    expect(r.to_a('//table_structure[@name="book"]/field').to_s).to eq("xxx")
  end

  it 'should be able to see newly created entity' do
    expect(@e.entities).to include('book')
    expect(@e.entity_get('book')).to eq(SIMPLE_SCHEME)
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
    schema = @e.entity_get('book')
    expect(@e.parse_data_with_schema(SIMPLE_RECORD, schema)).to eq({
        '_data' => "'{\\\"name\\\":\\\"Lorem ipsum\\\",\\\"yr\\\":1234}'",
        'name' => "'Lorem ipsum'",
        'yr' => 1234,
    })

    # FIXME: details about exception
    expect { @e.parse_data_with_schema({'foo' => 1234}, schema) }.to raise_error
  end

  it 'should be able to insert simple record in a given entity anonymously' do
    id = @e.insert('book', SIMPLE_RECORD)
    expect(id).to eq(1)
    expect(@e.entity_count('book')).to eq(1)
  end

  it 'should be able to retrieve inserted record' do
    rec = @e.get('book', 1)
    expect(rec).to eq(SIMPLE_RECORD)
  end

  it 'should be able to modify record' do
    @e.update('book', 1, SIMPLE_RECORD_2)
  end

  it 'should be able to delete entity' do
    @e.entity_delete('book')
    expect(@e.entities).not_to include('book')
  end
end
