# -*- encoding: utf-8 -*-

require File.expand_path("../lib/chie/version", __FILE__)
require 'date'

Gem::Specification.new { |s|
  s.name = 'chie'
  s.version = Chie::VERSION
  s.date = Date.today.to_s

  s.authors = ['Mikhail Yakshin']
  s.email = 'greycat.na.kor@gmail.com'

  s.homepage = 'https://github.com/GreyCat/chie'
  s.summary = 'Data-driven, document- and transaction-based object-relationship management system (ORM)'
  s.license = 'MIT'
  s.description = <<-EOF
Chie is a dynamic, data-driven, document- and transaction-based
object-relationship management system (ORM). It allows to create
entities with attributes and relations on the fly / in runtime, and
then insert, update, search for and retrieve entity instances. Every
action is written as a transaction, thus it's possible to revert to
specific version of entity at any time.
EOF

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.require_paths = ['lib']

  s.files = `git ls-files`.split("\n")
  s.executables = s.files.grep(%r{^bin/}) { |f| File.basename(f) }
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  s.add_development_dependency "bundler", "~> 1.3"
  s.add_development_dependency 'rake', '~> 10'
  s.add_development_dependency 'rspec', '~> 3'
  s.add_development_dependency 'codeclimate-test-reporter'

  s.add_dependency 'json', '~> 1.8'
  s.add_dependency 'mysql2', '~> 0.3.16'
}
