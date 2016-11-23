require 'active_record'
require_relative 'models/configuration'
require_relative 'datasift-ruby-utils/account_selector'

class AccountSelectorWithDbSupport < AccountSelector
  @@use_db = true
  cattr_accessor :use_db

  def self.select_from_commandline(account_selectors, options={})
    super(account_selectors, options)
  end

  def self.import_identity_file(path_to_identity_file=nil)
    @@use_db = false
    load_accounts(path_to_identity_file)
    configuration = Configuration.find_by_name("identities")
    configuration ||= Configuration.new(name: "identities")
    configuration.data = @@accounts
    configuration.save
    @@use_db = true
  end
  
protected
  def self.load_accounts(path_to_identity_file=nil)
    return unless @@accounts.empty?
    unless @@use_db
      super(path_to_identity_file)
    else
      configuration = Configuration.find_by_name("identities")
      unless configuration
        raise "Could not find identity configuration record"
      end
      @@accounts = configuration.data
    end
  end
end