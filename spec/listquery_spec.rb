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
  end

  it 'can run request without arguments' do
    q = ListQuery.new(@db, @entity, {})
    expect(q.run.count).to eq(5)
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
end
