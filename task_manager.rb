require 'datasift'
require_relative 'account_selector_with_db_support'
require 'yaml'
require 'active_support'
require 'active_support/core_ext/hash/keys.rb'
require_relative 'task_executor'
require 'terminal-table'

## usage
## ruby task_manager.rb <task> <command> <account_selector_string>

## With no options
## returns usage details

unless ARGV[0]
  puts "DataSift Task Manager
  Usage: ruby task_manager.rb <task> <command> <account_selector_string>
    Existing tasks:
      - stability
      - super_public
      - list
    All configuration settings for task_manager tasks are read from identities.yml"
  exit
end

OneOffDyno = true
DefaultTasks = { stability: [], super_public: [] }

time = Time.now

task, command, *account_selectors = ARGV
task = task.to_sym if task
command = command.to_sym if command && task != :import_identity_file

options = account_selectors.first if account_selectors && command == :super_public

TaskExecutor.initialize_active_record if OneOffDyno
master_config = AccountSelectorWithDbSupport.select_from_commandline(account_selectors, with_indexes: true, with_mongo: true) if account_selectors

unless [:stability, :super_public, :list, :import_identity_file].include?(task)
  puts "Unsupported task #{ task }"
  exit
end

if ([:subscribe, :unsubscribe].include?(command) && account_selectors) || task == :list
  if OneOffDyno
    tasks_record = Configuration.find_by_name("tasks")
    if tasks_record
      tasks = tasks_record.data
    else
      tasks = DefaultTasks
    end
  elsif File.exists?("tasks.yml")
    tasks = YAML.load_file("tasks.yml")
    tasks.deep_symbolize_keys!
  else
    tasks = DefaultTasks
  end
  if task == :list
    table = Terminal::Table.new({ :headings => ["Task", "Account", "Identity", "Index"] }) do |t|  
      tasks.each do |task, instances|
        next if instances.empty?
        instances.each do |instance|
          t.add_row [task, instance[:account], instance[:identity], instance[:index]]
        end
      end
    end
    puts "\nCurrent Subscriptions:"
    puts table
    exit
  end
  master_config.each do |account, identities|
    identities.each do |identity, info|
      info[:selected_indexes].each do |index|
        selected_task = { account: account, identity: identity, index: index }
        task_exists = tasks[task].include?(selected_task)
        if command == :subscribe
          unless task_exists
            tasks[task] << selected_task
            puts "#{ command }d to #{ task } for #{ account }:#{ identity }##{ index }"
          else
            puts "Subscription for #{ task } for #{ account }:#{ identity }##{ index } already exists"
          end
        elsif command == :unsubscribe
          if task_exists
            tasks[task].delete(selected_task)
            puts "#{ command }d from #{ task } for #{ account }:#{ identity }##{ index }"
          else
            puts "No subscription for #{ task } for #{ account }:#{ identity }##{ index } exists"
          end
        end
      end
    end
  end
  unless OneOffDyno
    tasks_file = File.open("tasks.yml", "w")
    tasks_file << YAML.dump(tasks)
  else
    tasks_record ||= Configuration.new({ name: "tasks" })
    tasks_record.data = tasks
    tasks_record.save
  end
  exit
end

TaskExecutor.initialize_active_record

case task
  when :import_identity_file
    AccountSelectorWithDbSupport.import_identity_file(command)
  when :stability
    return "<usage info>" unless command && account_selectors
    master_config.each do |account, identities|
      identities.each do |identity, info|
        puts info.inspect
        indexes = info[:info][:indexes].clone.delete_if { |index, v| !info[:selected_indexes].include?(index) }
        executor = TaskExecutor.new(time, account, identity, info[:config], indexes)
        executor.measure_stability
      end
    end
  when :super_public
    return "<usage info>" unless command && account_selectors
    master_config.each do |account, identities|
      identities.each do |identity, info|
        puts info.inspect
        indexes = info[:info][:indexes].clone.delete_if { |index, v| !info[:selected_indexes].include?(index) }
        executor = TaskExecutor.new(time, account, identity, info[:config], indexes)
        executor.record_super_public(info[:info][:mongo])
      end
    end
  else
    return "Unknown task type #{ task }"
end