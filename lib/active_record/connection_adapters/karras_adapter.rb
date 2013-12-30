require 'active_record/connection_adapters/abstract_adapter'

module ActiveRecord
  module ConnectionHandling # :nodoc:
    # Establishes a connection to the database that's used by all Active Record objects.
    def karras_connection(config)
      config = config.symbolize_keys
      connection = nil
      pool = nil
      #
      #config[:username] = 'root' if config[:username].nil?
      #
      #if Mysql2::Client.const_defined? :FOUND_ROWS
      #  config[:flags] = Mysql2::Client::FOUND_ROWS
      #end
      #
      #client = Mysql2::Client.new(config)
      #options = [config[:host], config[:username], config[:password], config[:database], config[:port], config[:socket], 0]
      ConnectionAdapters::KarrasAdapter.new(connection, logger, pool)
    end
  end
end


module ActiveRecord
  module ConnectionAdapters
    class KarrasAdapter < AbstractAdapter
      VERSION = "0.0.1"

    end
  end
end
