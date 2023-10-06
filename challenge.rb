# frozen_string_literal: true

require 'bundler/inline'

gemfile do
  source 'https://rubygems.org'
  gem 'activerecord'
  gem 'sqlite3'
end

require 'json'
require 'active_record'

USER_JSON = 'users.json'
COMPANY_JSON = 'companies.json'

ActiveRecord::Base.establish_connection(
  adapter: 'sqlite3',
  database: ':memory:'
)

ActiveRecord::Base.connection.execute <<-SQL
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY,
        name TEXT,
        top_up INTEGER,
        email_status BOOLEAN
    )
SQL

ActiveRecord::Base.connection.execute <<-SQL
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        company_id INTEGER,
        email_status BOOLEAN,
        active_status BOOLEAN,
        tokens INTEGER
    )
SQL

class User < ActiveRecord::Base
  belongs_to :company
end

class Company < ActiveRecord::Base
  has_many :users, -> { where(active_status: true).order(last_name: :asc) }

  def emailable
    if email_status
      users.where(email_status: true)
    else
      []
    end
  end

  def not_emailable
    users - emailable
  end
end

def seed_db
  users = JSON.parse(File.read(USER_JSON))
  companies = JSON.parse(File.read(COMPANY_JSON))

  companies.each do |company|
    Company.create!(
      id: company['id'],
      name: company['name'],
      top_up: company['top_up'],
      email_status: company['email_status']
    )
  end

  users.each do |user|
    User.create!(
      # Some users have duplicate IDs, ignore them since nothing
      # references user IDs
      # id: user['id'],
      first_name: user['first_name'],
      last_name: user['last_name'],
      email: user['email'],
      company_id: user['company_id'],
      email_status: user['email_status'],
      active_status: user['active_status'],
      tokens: user['tokens']
    )
  end
end

def generate_output
  Company.find_each do |company|
    top_up_total = 0
    puts "    Company Id: #{company.id}"
    puts "    Company Name: #{company.name}"
    puts '    Users Emailed:'
    company.emailable.each do |user|
      puts "        #{user.last_name}, #{user.first_name}, #{user.email}"
      puts "          Previous Token Balance, #{user.tokens}"
      user.tokens += company.top_up
      top_up_total += company.top_up
      user.save!
      puts "          New Token Balance #{user.tokens}"
    end
    puts '    Users Not Emailed:'
    company.not_emailable.each do |user|
      puts "        #{user.last_name}, #{user.first_name}, #{user.email}"
      puts "          Previous Token Balance, #{user.tokens}"
      user.tokens += company.top_up
      top_up_total += company.top_up
      user.save!
      puts "          New Token Balance #{user.tokens}"
    end
    puts "        Total amount of top ups for #{company.name}: #{top_up_total}"
    puts ''
  end
end

seed_db
generate_output