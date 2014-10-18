require 'spec_helper'

CREDENTIALS = {
  :host => 'localhost',
  :username => 'rdd_test',
  :password => 'rdd_test',
  :database => 'rdd_test',
}

describe Engine do
  it 'should connect to existing MySQL database' do
    e = Engine::connect_mysql(CREDENTIALS)
    expect(e).not_to be_nil
  end
end
