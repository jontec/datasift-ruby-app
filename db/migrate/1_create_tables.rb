# require 'active_record'
class CreateTables < ActiveRecord::Migration[5.0]
  def up
    create_table :transaction_logs do |t|
      t.timestamp :time
      t.string :task
      t.string :command
      t.string :account
      t.string :identity
      t.string :index
      t.string :index_id
      t.string :key
      t.string :value
    end
    create_table :measurements do |t|
      t.string :account
      t.string :identity
      t.string :name
      t.timestamp :time
      t.timestamp :obs_time
      t.integer :interactions
      t.integer :unique_authors
    end
    create_table :configurations do |t|
      t.string :name
      t.text :data
    end
  end

  def down
    drop_table :transaction_logs
    drop_table :measurements
    drop_table :configurations
  end
end