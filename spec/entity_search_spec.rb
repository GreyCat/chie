require 'spec_helper'

describe Entity do
  describe :search do
    SEARCH_ORG_SCHEME = {
      'attr' => [
        {'name' => 'name', 'type' => 'str', 'ind' => true},
      ]
    }

    SEARCH_PERSON_SCHEME = {
      'attr' => [
        {'name' => 'last_name', 'type' => 'str', 'ind' => true},
        {'name' => 'first_name', 'type' => 'str', 'ind' => true},
        {'name' => 'yr', 'type' => 'int', 'ind' => true},
      ],
      'rel' => [
        {'name' => 'employer', 'target' => 'org', 'type' => '0n'},
      ],
      'header' => ['last_name', 'first_name'],
    }

    SEARCH_BOOK_SCHEME = {
      'attr' => [
        {'name' => 'name', 'type' => 'str', 'ind' => true},
      ],
      'rel' => [
        {'name' => 'author', 'target' => 'person', 'type' => '0n'},
      ],
    }
    
    before(:all) do
      sqlclear
      @e = Engine.new(CREDENTIALS)

      @org = Entity.new('org', SEARCH_ORG_SCHEME)
      @e.entity_create(@org)

      @person = Entity.new('person', SEARCH_PERSON_SCHEME)
      @e.entity_create(@person)

      @book = Entity.new('book', SEARCH_BOOK_SCHEME)
      @e.entity_create(@book)
    end

    it 'can insert sample records' do
      @org.insert({'name' => 'Princeton'})
      @org.insert({'name' => 'Stanford'})
      @org.insert({'name' => 'Harvard'})
      @org.insert({'name' => 'Bell Labs'})

      @person.insert({'last_name' => 'Knuth', 'first_name' => 'Donald', 'yr' => 1938, 'employer' => [2]})
      @person.insert({'last_name' => 'Ritchie', 'first_name' => 'Dennis', 'yr' => 1941, 'employer' => [3, 4]})
      @person.insert({'last_name' => 'Kernighan', 'first_name' => 'Brian', 'yr' => 1942, 'employer' => [1, 4]})

      @book.insert({'name' => 'The Art of Computer Programming', 'author' => [1]})
      @book.insert({'name' => 'The C Programming Language', 'author' => [2, 3]})
    end

    it 'can do simple query without joins' do
      # What organizations end with "rd"?
      q = {
        fields: ['org._id', 'org.name'],
        where: [
          ['org.name', 'ends', 'rd'],
        ],
        order: ['org.name'],
      }
      expect(@org.search(q)).to eq([
        {'org._id' => 3, 'org.name' => 'Harvard'},
        {'org._id' => 2, 'org.name' => 'Stanford'},
      ])
    end

    it 'can do one-step backward join' do
      # What organizations employed Dennis Ritchie?
      q = {
        fields: ['org._id', 'org.name'],
        where: [
          ['person.last_name', 'eq', 'Ritchie'],
        ],
        order: ['org.name'],
      }
      expect(@org.search(q)).to eq([
        {'org._id' => 4, 'org.name' => 'Bell Labs'},
        {'org._id' => 3, 'org.name' => 'Harvard'},
      ])
    end
  end
end
