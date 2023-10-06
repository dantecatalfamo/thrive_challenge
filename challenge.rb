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

## Setup database connection and schema

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
        tokens INTEGER,
        UNIQUE(company_id, email)
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

## Methods

def run
  users = JSON.parse(File.read(USER_JSON))
  companies = JSON.parse(File.read(COMPANY_JSON))

  seed_db(users, companies)
  top_up_and_generate_output
end

# Seeds the DB with the provided JSON files.
#
# All insertions are done in a single transaction, in case there are
# any errors related to constraints like a recurring ID. This is done
# to prevent inconsistent database state where half the records are loaded.
#
# If the seeding runs into any errors, the entire transaction is
# aborted and the offending record is reported so it can be corrected.
#
# User IDs in the provided JSON are ignored.

def seed_db(users, companies)
  ActiveRecord::Base.transaction do
    companies.each do |company|
      Company.create!(
        id: company['id'],
        name: company['name'],
        top_up: company['top_up'],
        email_status: company['email_status']
      )
    rescue StandardError => e
      puts "Failed to insert Company: #{e}"
      puts "Offending record: #{user}"
      exit 1
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
    rescue StandardError => e
      puts "Failed to insert User: #{e}"
      puts "Offending record: #{user}"
      exit 1
    end
  end
end

# Top up all users that are active and associated with a company, and
# send emails to users who have email_status and work for companies
# with email_status.
#
# Print out the changes made and who emails were sent to stdout
#
# All modifications in this method take place inside of a single
# transaction. If there is an error, we don't want to leave the
# database in an inconsistent state where some overs have been topped
# up and others haven't, as it could lead to some users being topped
# up twice when the job is re-run.

def top_up_and_generate_output
  ActiveRecord::Base.transaction do
    puts ''
    Company.find_each do |company|
      next if company.users.empty?

      top_up_total = 0
      puts "\tCompany Id: #{company.id}"
      puts "\tCompany Name: #{company.name}"
      puts "\tUsers Emailed:"
      company.emailable.each do |user|
        puts "\t\t#{user.last_name}, #{user.first_name}, #{user.email}"
        puts "\t\t  Previous Token Balance, #{user.tokens}"
        puts "\t\t  New Token Balance #{user.tokens + company.top_up}"
        user.tokens += company.top_up
        top_up_total += company.top_up
        user.save!
      end
      puts "\tUsers Not Emailed:"
      company.not_emailable.each do |user|
        puts "\t\t#{user.last_name}, #{user.first_name}, #{user.email}"
        puts "\t\t  Previous Token Balance, #{user.tokens}"
        puts "\t\t  New Token Balance #{user.tokens + company.top_up}"
        user.tokens += company.top_up
        top_up_total += company.top_up
        user.save!
      end
      puts "\t\tTotal amount of top ups for #{company.name}: #{top_up_total}"
      puts ''
    end
  end
end

run
