require 'csv'
require 'active_support'
require 'active_support/core_ext/integer'
require 'datasift'
require 'mongo'

# active record dependencies (TODO: make this skip ActiveRecord when it's not installed)
require 'active_record'
require 'activerecord-import'
require 'zlib'
include ActiveRecord::Tasks
require_relative 'models/transaction_log'
require_relative 'models/measurement'
require_relative 'models/configuration'

class TaskExecutor
  attr_reader :log, :account, :identity, :indexes, :time
  attr_accessor :mongo
  Log_Filename = "task_executor.log"

  def initialize(time, account, identity, config, indexes={}, options={})
    @time, @account, @identity = [time.gmtime, account, identity]
    @indexes = indexes unless indexes.empty?

    @client = DataSift::Pylon.new(config)

    options[:db] ||= true
    if options[:db]
      # initialize connections for ActiveRecord
      @log_entries = []
      @db = true
    else
      @log_to_file = true
      log_exists = File.exists?(Log_Filename)
      @log = File.open(Log_Filename, "a+")
      @log.puts CSV.generate_line ["time", "task", "command", "account", "identity", "index", "index_id", "key", "value"] unless log_exists
    end
  end

  def measure_stability
    @task, @command = :stability, :execute
    # Implement stability in single file for all indexes
    # File.mkdir("db/stability") unless File.exists?("db/stability")
    stability_filename = "data/stability.csv"
    file_exists = File.exists?(stability_filename)
    file = File.open(stability_filename, "a+")

    file.puts CSV.generate_line ["account", "identity", "name", "time", "obs_time", "interactions", "unique_authors"] unless file_exists

    # Implement synchronized timing
    time = Time.gm(@time.year, @time.month, @time.day, @time.hour, 0, 0)
    start_time = time - (3600 * 48)
    start_time = Time.gm(start_time.year, start_time.month, start_time.day, start_time.hour, 0, 0)
    time_series_params = { analysis_type: "timeSeries", parameters: { interval: "hour" }}

    measurements = []

    indexes.each do |name, index_id|
      @index, @index_id = name, index_id
      response = @client.analyze('', time_series_params, '', start_time.to_i.to_s, time.to_i.to_s, index_id)
      if response[:data][:analysis][:redacted]
        record :redacted, true
        next
      end
      record :redacted, false
      results = response[:data][:analysis][:results]
      daily_volume = 0
      current_volume = 0
      comparison_time = time - 1.days
      results.each do |data_point|
        daily_volume += data_point[:interactions] if Time.at(data_point[:key]).gmtime >= comparison_time
        current_volume = data_point[:interactions] if Time.at(data_point[:key]).gmtime == time - 1.hours
        unless @db
          file.puts CSV.generate_line([@account, @identity, name, format_time(data_point[:key]), format_time(time), data_point[:interactions], data_point[:unique_authors]])
        else
          measurements << Measurement.new({
            account: @account, identity: @identity, name: name, time: Time.at(data_point[:key]).gmtime,
            obs_time: time, interactions: data_point[:interactions], unique_authors: data_point[:unique_authors]
          })
        end
      end
      record :daily_volume, daily_volume
      record :current_volume, current_volume
    end
    Measurement.import(measurements) if @db && !measurements.empty?
    @task, @command, @index = nil, nil, nil
    flush_log
  end

  def record_super_public(mongo)
    @task, @command = :super_public, :execute
    begin
      mongo_client = Mongo::Client.new("mongodb://#{ mongo[:username] }:#{ mongo[:password] }@#{ mongo[:host] }/#{ mongo[:database] }")
    rescue => e
      record :mongo_connection_error, e
      return
    else
      record :mongo_connection_success
    end
    indexes.each do |index, index_id|
      @index, @index_id = index, index_id
      collection_name = [@account, @identity, @index].join("_").to_sym
      collection = mongo_client[collection_name]
      record :mongo_collection_error, collection_name unless collection

      record_count = 10
      csdl_filter = ''
      begin
        response = @client.sample('', record_count, nil, nil, csdl_filter, index_id)
      rescue => e
        record :pylon_sample_error, e
        return
      end
      if response[:data]
        consumed = response[:data][:interactions].length
        begin
          collection.insert_one({ interactions: response[:data][:interactions], count: consumed, timestamp: Time.now.to_i })
          # collection.insert_many(response[:data][:interactions])
        rescue => e
          record :mongo_load_error, e
          return
        else
          record :mongo_load_success 
        end
      else
        consumed = 0
      end
      record :super_public_consumed, consumed
      @index, @index_id = nil, nil
    end
    flush_log
  end

  def self.initialize_active_record
    generate_active_record_params
    DatabaseTasks.database_configuration = @ar_config
    DatabaseTasks.db_dir = 'db'
    DatabaseTasks.migrations_paths = ['db/migrate']
    DatabaseTasks.env = ENV['environment'] || :production
    ActiveRecord::Base.establish_connection(@ar_config)
  end

  def self.migrate
    DatabaseTasks.migrate
  end

protected
  def self.generate_active_record_params
    db_string = ENV['DATABASE_URL']
    config = {}
    keys = [:adapter, :username, :password, :host, :port, :database]
    db_string.match(/(postgres):\/\/([^:]+):([^@]+)@([^:]+):(\d+)\/(.+)$/).captures.each_with_index do |val, i|
      config[keys[i]] = val
    end
    config[:host] = config[:host] + ":" + config.delete(:port)
    @ar_config = config
    @ar_config[:adapter] = "postgresql"
    # @ar_config[:host] = ""
  end

  def log(time, task, command, account, identity, index, index_id, key, value)
    if @db
      @log_entries << TransactionLog.new({
        time: time, task: task, command: command, account: account, identity: identity,
        index: index, index_id: index_id, key: key, value: value
      })
    else
      @log_file.puts CSV.generate_line [time, task, command, account, identity, index, index_id, key, value]
    end
  end

  def record(key, value="")
    log @time, @task, @command, @account, @identity, @index, @index_id, key, value
  end

  def format_time(t)
    str = "%Y-%b-%d %H:%M:%S"
    if t.is_a?(Time)
      t.strftime(str)
    else
      Time.at(t).gmtime.strftime(str)
    end
  end

  def flush_log
    return unless @db
    TransactionLog.import(@log_entries) unless @log_entries.empty?
    @log_entries = []
  end
end