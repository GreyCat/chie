require 'spec_helper'

SIMPLE_SCHEME = {
  'attr' => [
    {'name' => 'name', 'type' => 'str', 'len' => 100, 'ind' => true},
    {'name' => 'yr', 'type' => 'int', 'ind' => true},
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
    expect(@book.instance_eval { generate_sql_columns(SIMPLE_RECORD) }).to eq({
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
    SIMPLE_RECORD.each_pair { |k, v| expect(rec[k]).to eq(v) }
  end

  it 'should be able to modify record' do
    @book.update(1, SIMPLE_RECORD_2)
    rec = @book.get(1)
    SIMPLE_RECORD_2.each_pair { |k, v| expect(rec[k]).to eq(v) }
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

  it 'should be not make new history entry when saving the same record twice' do
    @book.update(1, SIMPLE_RECORD_2)
    hist = @book.history_list(1)
    expect(hist.size).to eq(2)
  end

  USER_ID = 1234

  it 'should add another record with given user and time' do
    @book.insert(SIMPLE_RECORD, USER_ID, Time.at(1000000000))
  end

  it 'should have correct user and time in history for that record' do
    hist = @book.history_list(2)
    expect(hist.size).to eq(1)
    hist.each { |entry|
      expect(entry['user_id']).to eq(USER_ID)
      expect(Time.at(entry['ts'])).to eq(Time.at(1000000000))
    }
  end

  it 'should be able to list all entities with no where phrase' do
    expect(@book.list.count).to eq(2)
    expect(@book.count).to eq(2)
  end

  it 'should be able to list all entities with empty where phrase' do
    expect(@book.list(where: {}).count).to eq(2)
    expect(@book.count(where: {})).to eq(2)
  end

  it 'should be able to list entities by name and year' do
    expect(@book.list(where: {'name' => 'foo'}).count).to eq(0)
    expect(@book.count(where: {'name' => 'foo'})).to eq(0)
    expect(@book.list(where: {'name' => SIMPLE_RECORD['name']}).count).to eq(1)
    expect(@book.count(where: {'name' => SIMPLE_RECORD['name']})).to eq(1)
    expect(@book.list(where: {'name' => SIMPLE_RECORD_2['name']}).count).to eq(1)
    expect(@book.count(where: {'name' => SIMPLE_RECORD_2['name']})).to eq(1)
    expect(@book.list(where: {'yr' => SIMPLE_RECORD['yr']}).count).to eq(1)
    expect(@book.count(where: {'yr' => SIMPLE_RECORD['yr']})).to eq(1)
    expect(@book.list(where: {'yr' => SIMPLE_RECORD_2['yr']}).count).to eq(1)
    expect(@book.count(where: {'yr' => SIMPLE_RECORD_2['yr']})).to eq(1)
  end

  it 'should be able to list entities by name with LIKE operator' do
    expect(@book.list(where: {'name' => ['LIKE', 'foo']}).count).to eq(0)
    expect(@book.count(where: {'name' => ['LIKE', 'foo']})).to eq(0)
    expect(@book.list(where: {'name' => ['LIKE', 'Lor%']}).count).to eq(1)
    expect(@book.count(where: {'name' => ['LIKE', 'Lor%']})).to eq(1)
    expect(@book.list(where: {'name' => ['LIKE', '%i%']}).count).to eq(2)
    expect(@book.count(where: {'name' => ['LIKE', '%i%']})).to eq(2)
  end

  context 'book->source relation' do
    SOURCE_SCHEME = {
      'attr' => [
        {
          'name' => 'name',
          'type' => 'str',
          'len' => 100,
          'ind' => true,
        }
      ]
    }

    ARTICLE_SCHEME = {
      'attr' => [
        {
          'name' => 'name',
          'type' => 'str',
          'len' => 100,
          'ind' => true,
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

    it 'should be able to insert record in source entity' do
      @source = @e.entity('source')
      @source.insert('name' => 'Source')
      expect(@source.count).to eq(1)
    end

    it 'should not be able to insert source-less record in article entity' do
      @article = @e.entity('article')
      expect { @article.insert('name' => 'Unbound article') }.to raise_error(ValidationError)
      expect(@article.count).to eq(0)
    end

    it 'should be able to insert sourced record in article entity' do
      @article = @e.entity('article')
      @article.insert('name' => 'Sourced article', 'source' => 1)
      expect(@article.count).to eq(1)
    end

    it 'should be able to see inserted record with linked one' do
      @article = @e.entity('article')
      a = @article.get(1)
      expect(a).to eq({
        "_header" => "Sourced article",
        "name" => "Sourced article",
        "source"=> [{
          "_id" => 1,
          "_header" => "Source",
        }],
      })
    end

    it 'should be able to insert more sources and articles' do
      @source = @e.entity('source')
      @source.insert('name' => 'Source 2')

      @article = @e.entity('article')
      @article.insert('name' => 'Another article from Source', 'source' => 1)
      @article.insert('name' => 'Article from Source 2', 'source' => 2)

      expect(@source.count).to eq(2)
      expect(@article.count).to eq(3)
    end

    it 'should be able to list articles by source' do
      @article = @e.entity('article')
      expect(@article.list(where: {'source' => 1}).count).to eq(2)
      expect(@article.list(where: {'source' => 2}).count).to eq(1)
    end
  end

  context 'book->source relation, reloaded' do
    before(:all) do
      @e = Engine.new(CREDENTIALS)
    end

    it 'should be able to see previously inserted record with linked one' do
      @article = @e.entity('article')
      a = @article.get(1)
      expect(a).to eq({
        "_header" => "Sourced article",
        "name" => "Sourced article",
        "source"=> [{
          "_id" => 1,
          "_header" => "Source",
        }],
      })
    end
  end

  context 'person<->book multi relation' do
    PERSON_SCHEME = {
      'attr' => [
        {'name' => 'name', 'type' => 'str', 'ind' => true},
      ]
    }

    BOOK_SCHEME = {
      'attr' => [
        {'name' => 'name', 'type' => 'str', 'ind' => true},
      ],
      'rel' => [
        {'name' => 'author', 'target' => 'person', 'type' => '0n'},
      ]
    }

    before(:all) do
      sqlclear
      @e = Engine.new(CREDENTIALS)
    end

    it 'should be able to create two related entities' do
      @person = @e.entity_create(Entity.new('person', PERSON_SCHEME))
      @book = @e.entity_create(Entity.new('book', BOOK_SCHEME))

      expect(@e.entity('person')).to eq(@person)
      expect(@e.entity('book')).to eq(@book)
    end

    it 'should be able to insert 2 persons' do
      @person = @e.entity('person')
      @person.insert('name' => 'Person 1')
      @person.insert('name' => 'Person 2')
      expect(@person.count).to eq(2)
    end

    it 'should be able to insert book without author' do
      @book = @e.entity('book')
      @book.insert('name' => 'Anonymous book')
      expect(@book.count).to eq(1)
    end

    it 'should be able to insert book with author #1' do
      @book = @e.entity('book')
      @book.insert('name' => 'Book #2', 'author' => [1])
      expect(@book.count).to eq(2)
    end

    it 'should be able to insert book co-authored by persons #1 and #2' do
      @book = @e.entity('book')
      @book.insert('name' => 'Co-authored #1', 'author' => [1, 2])
      expect(@book.count).to eq(3)
    end

    it 'should be able to insert another co-authored book' do
      @book = @e.entity('book')
      @book.insert('name' => 'Co-authored #2', 'author' => [1, 2])
      expect(@book.count).to eq(4)
    end

    it 'should return properly all books by person #1' do
      @book = @e.entity('book')
      book_ids = []
      @book.list(:where => {'author' => 1}).each { |rec|
        book_ids << rec['_id']
      }
      book_ids.sort!
      expect(book_ids).to eq([2, 3, 4])
    end

    it 'should be able to update author of Book #2 to another single author' do
      @book = @e.entity('book')
      @book.update(2, 'name' => 'Book #2', 'author' => [2])
    end

    it 'should be able to update author of Book #2 to co-authored' do
      @book = @e.entity('book')
      @book.update(2, 'name' => 'Book #2', 'author' => [1, 2])
    end
  end

  context 'header fields handling' do
    INVALID_SCHEME = {
      'attr' => [
        {'name' => 'filename', 'type' => 'str', 'ind' => true},
      ],
    }

    before(:all) do
      sqlclear
      @e = Engine.new(CREDENTIALS)
    end

    it 'should refuse to create entity without default header field "name"' do
      expect {
        @book = Entity.new('book', INVALID_SCHEME)
        @e.entity_create(@book)
      }.to raise_error
    end

    context 'one custom header field entity' do
      SCHEME_1COL = {
        'attr' => [
          {'name' => 'filename', 'type' => 'str', 'ind' => true},
        ],
        'header' => ['filename'],
      }

      it 'can create entity' do
        @file = Entity.new('file', SCHEME_1COL)
        @e.entity_create(@file)
      end

      it 'can insert record into one custom header entity' do
        @file = @e.entity('file')
        @file.insert('filename' => 'Foo')
        expect(@file.count).to eq(1)
      end

      it 'can get proper header for one record' do
        @file = @e.entity('file')
        r = @file.get(1)
        expect(r['_header']).to eq('Foo')
      end

      it 'can see proper headers in listing' do
        @file = @e.entity('file')
        @file.list.each { |row|
          expect(row.header).to eq('Foo')
        }
      end
    end

    context 'two custom header fields entity' do
      SCHEME_2COL = {
        'attr' => [
          {'name' => 'first_name', 'type' => 'str', 'ind' => true},
          {'name' => 'last_name', 'type' => 'str', 'ind' => true},
        ],
        'header' => ['last_name', 'first_name'],
      }

      it 'can create entity' do
        @person = Entity.new('person', SCHEME_2COL)
        @e.entity_create(@person)
      end

      it 'can insert record into one custom header entity' do
        @person = @e.entity('person')
        @person.insert('first_name' => 'John', 'last_name' => 'Smith')
        @person.insert('first_name' => 'Bill', 'last_name' => 'Allen')
        @person.insert('first_name' => 'William', 'last_name' => 'Harris')
        expect(@person.count).to eq(3)
      end

      it 'can get proper header for one record' do
        @person = @e.entity('person')
        r = @person.get(1)
        expect(r['_header']).to eq('Smith John')
      end

      it 'can see proper headers in listing' do
        @person = @e.entity('person')
        r = []
        @person.list.each { |row| r << row.header }
        expect(r).to eq([
          'Allen Bill',
          'Harris William',
          'Smith John',
        ])
      end
    end

    context 'book <-> person with two custom header fields' do
      SCHEME_BOOK = {
        'attr' => [
          {'name' => 'heading', 'type' => 'str', 'ind' => true},
          {'name' => 'yr', 'type' => 'int', 'ind' => true},
        ],
        'header' => ['heading', 'yr'],
        'rel' => [
          {'name' => 'author', 'target' => 'person', 'type' => '0n'}
        ],
      }

      it 'can create linked entity' do
        @book = Entity.new('book', SCHEME_BOOK)
        @e.entity_create(@book)
      end

      it 'can insert authored book' do
        @book = @e.entity('book')
        @book.insert('heading' => 'About foo', 'yr' => 2000, 'author' => [1, 2])
        expect(@book.count).to eq(1)
      end

      it 'can properly get record back with all entities resolved' do
        @book = @e.entity('book')
        rec = @book.get(1)
        expect(rec['_header']).to eq('About foo 2000')
        expect(rec['author']).to eq([
          {'_id' => 1, '_header' => 'Smith John'},
          {'_id' => 2, '_header' => 'Allen Bill'},
        ])
      end
    end
  end
end
