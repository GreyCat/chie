# Chie

Chie is Ruby implementation of data-driven, document- and
transaction-based object-relationship management system (ORM).

## Why another ORM?

To put it simply, most traditional web applications run in a fixed
structure of database. That's what traditional ORMs emphasize and
that's what "M" (the Model) stands for in MVC paradigm: one gets
roughly 1-to-1 mapping from tables in one's database to fixed classes
(that wrap these tables) as "models".

That's the familiar example in ActiveRecord:

    class Product < ActiveRecord::Base
      belongs_to :vendor
    end

    class Vendor < ActiveRecord::Base
      has_many :products
    end

The field list (mapped to accessor object methods here) would be
derived from database here, but note that:

* relations ("associations" in terms of ActiveRecord) are basically
  hard-coded in the model
* one doesn't have any direct methods to add / remove fields in
  run-time; fields are typically created by migrations ran during
  installation phase of the project

It's ok for most typical applications, but some applications require
their databases to be more dynamic: for example, if one wants to have
multiple (thousands) of tables for different kinds of products, each
sporting a couple of dozens of searchable (=indexed) attributes.

## Generic API usage

TODO

## Gory internal details

TODO

## Testing

Chie is extensively covered by RSpec-based tests. Just run `rspec` to
run it. There's only one requirement: Chie must be able to connect to
local MySQL database `chie_test` using user `chie_test` and password
`chie_test`.

This can be accomplished by issuing the following commands in MySQL
console (obviously authorized as a user that is able to create
databases and grant privileges to others):

```
CREATE DATABASE chie_test;
USE chie_test;
GRANT ALL PRIVILEGES ON chie_test.* TO 'chie_test'@'localhost' IDENTIFIED BY 'chie_test';
FLUSH PRIVILEGES;
```

## License

MIT
