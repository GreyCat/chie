require 'spec_helper'

require 'mysql2'

SCHEME = {
  'attr' => [
    {'name' => 'name', 'type' => 'str', 'len' => 100, 'ind' => true},
    {'name' => 'yr', 'type' => 'int', 'ind' => true},
  ]
}

describe ListQuery do
  before(:all) do
    sqlclear
    @engine = Engine.new(CREDENTIALS)
    @db = @engine.instance_eval('@db')
    @entity = Entity.new('book', SCHEME)
    @engine.entity_create(@entity)

    @entity.insert({'name' => 'Alpha', 'yr' => 1912})
    @entity.insert({'name' => 'Beta', 'yr' => 2005})
    @entity.insert({'name' => 'Charlie', 'yr' => 1980})
    @entity.insert({'name' => 'Delta', 'yr' => 1983})
    @entity.insert({'name' => 'Echo', 'yr' => 1989})
    @entity.insert({'name' => 'Foxtrot', 'yr' => 2000})
    @entity.delete(6)
  end

  it 'can run request without arguments' do
    q = ListQuery.new(@db, @entity, {})
    expect(q.run.count).to eq(5)
  end

  it 'can run request without filtering, allowing deleted' do
    q = ListQuery.new(@db, @entity, deleted: true)
    expect(q.run.count).to eq(6)
  end

  it 'can list by exact name' do
    q = ListQuery.new(@db, @entity, where: {'name' => 'Alpha'})
    expect(q.run.count).to eq(1)
    q = ListQuery.new(@db, @entity, where: {'name' => 'weird name'})
    expect(q.run.count).to eq(0)
  end

  it 'can list by exact integer' do
    q = ListQuery.new(@db, @entity, where: {'yr' => 1980})
    expect(q.run.count).to eq(1)
  end

  it 'can do operator match against integers' do
    q = ListQuery.new(@db, @entity, where: {'yr' => ['<', 1980]})
    expect(q.run.count).to eq(1)
    q = ListQuery.new(@db, @entity, where: {'yr' => ['<=', 1980]})
    expect(q.run.count).to eq(2)
    q = ListQuery.new(@db, @entity, where: {'yr' => ['<>', 1980]})
    expect(q.run.count).to eq(4)
    q = ListQuery.new(@db, @entity, where: {'yr' => ['>=', 1980]})
    expect(q.run.count).to eq(4)
    q = ListQuery.new(@db, @entity, where: {'yr' => ['>', 1980]})
    expect(q.run.count).to eq(3)
  end

  it 'can do match against ranges' do
    q = ListQuery.new(@db, @entity, where: {'yr' => (1980..1985)})
    expect(q.where_phrase).to eq('WHERE `yr` BETWEEN 1980 AND 1985 AND `book`._deleted=0')
    expect(q.run.count).to eq(2)

    q = ListQuery.new(@db, @entity, where: {'yr' => (1980..Float::INFINITY)})
    expect(q.where_phrase).to eq('WHERE `yr` >= 1980 AND `book`._deleted=0')
    expect(q.run.count).to eq(4)

    q = ListQuery.new(@db, @entity, where: {'yr' => (-Float::INFINITY..1980)})
    expect(q.where_phrase).to eq('WHERE `yr` <= 1980 AND `book`._deleted=0')
    expect(q.run.count).to eq(2)

    q = ListQuery.new(@db, @entity, where: {'yr' => (-Float::INFINITY..Float::INFINITY)})
    expect(q.run.count).to eq(5)
  end

  it 'can do IN matches against multiple values' do
    q = ListQuery.new(@db, @entity, where: {'yr' => ['IN', [1980, 1982, 1983]]})
    expect(q.where_phrase).to eq('WHERE `yr` IN (1980,1982,1983) AND `book`._deleted=0')
    expect(q.run.count).to eq(2)
  end

  it 'can do IN matches against empty lists' do
    q = ListQuery.new(@db, @entity, where: {'yr' => ['IN', []]})
    expect(q.where_phrase).to eq('WHERE 1=0 AND `book`._deleted=0')
    expect(q.run.count).to eq(0)
  end

  it 'can do group counts for a given attribute' do
    q = ListQuery.new(@db, @entity)
    g = q.group_count('yr')
    expect(g).to eq({
      1912 => 1,
      1980 => 1,
      1983 => 1,
      1989 => 1,
      2005 => 1,
    })
  end

  it 'orders lists by single field name as string' do
    q = ListQuery.new(@db, @entity, order_by: 'yr')
    expect(q.order_by).to eq('`book`.`yr`')
    expect(q.run.map { |row| row['yr'] }).to eq([
      1912,
      1980,
      1983,
      1989,
      2005,
    ])
  end

  it 'orders lists by single field name as array' do
    q = ListQuery.new(@db, @entity, order_by: ['yr'])
    expect(q.order_by).to eq('`book`.`yr`')
    expect(q.run.map { |row| row['yr'] }).to eq([
      1912,
      1980,
      1983,
      1989,
      2005,
    ])
  end

  it 'orders list by multiple field names' do
    q = ListQuery.new(@db, @entity, order_by: ['yr', 'name'])
    expect(q.order_by).to eq('`book`.`yr`,`book`.`name`')
  end

  it 'orders list by arbitrary string expression' do
    q = ListQuery.new(@db, @entity, order_by: '10000 - yr')
    expect(q.order_by).to eq('10000 - yr')
  end

  it 'orders list by arbitrary array expression' do
    q = ListQuery.new(@db, @entity, order_by: ['10000 - yr'])
    expect(q.order_by).to eq('10000 - yr')
  end
end
