# require 'active_record'
# require 'zlib'
# include ActiveRecord::Tasks
require_relative '../task_executor'

namespace :db do
  task :configuration do
    TaskExecutor.initialize_active_record
  end
  task :migrate => :configuration do
    TaskExecutor.migrate
  end
end